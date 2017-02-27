package kmeans;

import java.io.BufferedWriter;
import java.io.IOException;
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

public class Q3 {

	private static String input;
	private static String output;
	private static int k;
	static Random random = new Random();
	final static String CENTERFILE = "/home/caitlin/git/BigData/data/centroids";
	final static int MAX_ITERATIONS = 5;

	public static void main(String[] args) throws Exception {

		// parse args
		try {
			input = args[0];
			output = args[1];
			k = Integer.parseInt(args[2]);

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
			OutputStream centers = fs.create(new Path(strFSName + CENTERFILE));
			bw = new BufferedWriter(new OutputStreamWriter(centers));

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
			DistributedCache.addCacheFile(new URI(CENTERFILE), conf);

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

		}

	}

}
