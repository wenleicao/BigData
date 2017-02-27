package kmeans;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver for iterative MapReduce implementation of KMeans algorithm.
 * @author caitlin
 *
 */

public class Q3 {

	private static String input;
	private static String centers;
	private static String output;
	private static int k;
	static Random random = new Random();
	final static int MAX_ITERATIONS = 5;

	
	/**
	 * @param args usage: inputfile, centerfile, outputfile, k
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// parse args
		try {
			input = args[0];
			centers = args[1];
			output = args[2];
			k = Integer.parseInt(args[3]);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		Configuration conf = new Configuration();

		// set k
		conf.setInt("k", k);

		// delete old output directories if they exist
		FileSystem fs = FileSystem.get(conf);
		String strFSName = conf.get("fs.default.name");

		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}

		// generate k random centers
		BufferedWriter bw = null;
		try {

			int x;
			int y;
			// write out pivot, bound, size file
			OutputStream os = fs.create(new Path(centers));
			bw = new BufferedWriter(new OutputStreamWriter(os));

			for (int i = 1; i < k + 1; i++) {

				x = random.nextInt(10000);
				y = random.nextInt(10000);
				bw.write(x + "," + y + "\n");
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Generated " + k + " random centers.");
		System.err.println("Generated " + k + " random centers.");

		int numIterations = 0;

		// Start Kmeans iterations

		while (numIterations < MAX_ITERATIONS) {

			// put centroid file in distributed cache
			DistributedCache.addCacheFile(new URI(centers), conf);

			Job job = Job.getInstance(conf, "Query2");
			job.setJarByClass(Q3.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(KMeansMapper.class);
			job.setCombinerClass(KMeansCombiner.class);
			job.setReducerClass(KMeansReducer.class);

			job.setMapOutputKeyClass(PointWritable.class);
			job.setMapOutputValueClass(PointWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			// use single reducer optimization
			job.setNumReduceTasks(1);

			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			System.exit(job.waitForCompletion(true) ? 0 : 1);

			numIterations++;

			// get output
			InputStream is = null;
			BufferedReader br = null;
			boolean hasChanged = true;
			
			try {
				is = new BufferedInputStream(fs.open(new Path(output
						+ "part-r-00000")));
				br = new BufferedReader(new InputStreamReader(is));
				Path centerPath = new Path(strFSName + centers);
				// delete old centroid file
				fs.delete(centerPath, true);
				OutputStream os = fs.create(centerPath);
				bw = new BufferedWriter(new OutputStreamWriter(os));
//				get new centroids
				for (int i=0; i<k+1 ; i++){
					bw.write(br.readLine());
				}
//				check if condition to stop has been met
				String s = br.readLine();
				hasChanged = Boolean.parseBoolean(s);
				br.close();
				bw.close();
				is.close();
				os.close();

			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (bw != null) {
						bw.close();
					}
					if (br != null) {
						br.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(!hasChanged){
//				the centroids id not change in the last iteration, so stop
				break;
			}
		}
	}
}
