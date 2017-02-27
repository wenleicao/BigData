package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KMeansMapper extends
		Mapper<LongWritable, Text, PointWritable, PointWritable> {

	private PointWritable centroid = new PointWritable();
	private PointWritable point = new PointWritable();
	private PointWritable[] centroids;
	private int k;

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

	private PointWritable closestCentroid(PointWritable point) {
		double closestDist = Float.MAX_VALUE;
		PointWritable closestCentroid = null;

		for (PointWritable centroid : centroids) {
			double dist = point.distTo(centroid);
			if (closestDist > dist) {
				closestDist = dist;
				closestCentroid = centroid;
			}
		}
		return closestCentroid;
	}
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		point = new PointWritable(value.toString(),1);
		centroid = closestCentroid(point);
		context.write(centroid, point);
	}
}