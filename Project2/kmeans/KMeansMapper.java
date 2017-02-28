package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
		centroids = readCentroids(conf, k);
	}

	/**
	 * read centroids from a single file.
	 */
	public PointWritable[] readCentroids(
			Configuration conf, int k) throws IOException {

		FileSystem fs = FileSystem.get(conf);
		URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
		Path getPath = new Path(cacheFiles[0].getPath());
		BufferedReader bf = null;
		try {
			bf = new BufferedReader(new InputStreamReader(
					fs.open(getPath)));
			String line;
			int i = 0;
			while ((line = bf.readLine()) != null) {
				centroids[i] = new PointWritable(line);
				centroids[i].setId(i);
				i++;
			}
			return centroids;
		} catch (IOException ioe) {
			System.err
					.println("Caught exception while parsing the cached file");
			return null;
		} finally {
			if (bf != null) {
				bf.close();
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

		point = new PointWritable(value.toString(), 1);
		centroid = closestCentroid(point);
		context.write(centroid, point);
	}
}