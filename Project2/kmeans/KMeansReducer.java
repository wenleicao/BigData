package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KMeansReducer extends
		Reducer<PointWritable, PointWritable, Text, NullWritable> {

	private PointWritable[] centroids;
	PointWritable newCentroid = new PointWritable();
	Text output = new Text();
	static int k;
	static boolean hasChanged = false;

	/**
	 * Initialize fields once per mapper.
	 */
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		k = conf.getInt("k", 10);
		centroids = new PointWritable[k];

		// read centroids
		Path[] localFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		if (localFiles == null || localFiles.length < 1)
			throw new IOException("No centroids are provided!");

		for (Path path : localFiles) {
			String filename = path.toString();
			if (filename.endsWith("centroids")) {
				centroids = readCentroids(filename, conf, k);
			}
		}
	}

	/**
	 * read centroids from a single file.
	 */
	public PointWritable[] readCentroids(String centroidFile,
			Configuration conf, int k) throws IOException {

		PointWritable[] centroids = new PointWritable[k];
		FileSystem fs = FileSystem.get(conf);
		BufferedReader fis = null;
		FSDataInputStream in = fs.open(new Path(centroidFile));
		try {
			fis = new BufferedReader(new InputStreamReader(in));
			String line;
			int i = 0;
			while ((line = fis.readLine()) != null) {
				centroids[i] = new PointWritable(line);
				i++;
			}
			return centroids;
		} catch (IOException ioe) {
			System.err
					.println("Caught exception while parsing the cached file '"
							+ centroidFile + "'");
			return null;
		} finally {
			if (in != null) {
				fis.close();
				in.close();
			}
		}
	}

	/**
	 * compute new centroid.
	 */
	public void reduce(PointWritable key, Iterable<PointWritable> values,
			Context context) throws IOException, InterruptedException {

		newCentroid.clear();
		for (PointWritable p : values) {
			newCentroid.addPoint(p);
		}
		
//		check if centroid has changed
		PointWritable oldCentroid = centroids[key.id];
		if(!(oldCentroid.x == newCentroid.x && oldCentroid.y==newCentroid.y)){
			hasChanged = true;
		}
		output.set(newCentroid.toString());
		context.write(output, NullWritable.get());
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		if(hasChanged){
			output.set(String.valueOf(hasChanged));
			context.write(output, NullWritable.get());
		}
	}
}
