package kmeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCombiner extends
		Reducer<PointWritable, PointWritable, PointWritable, PointWritable> {

//	send all data to one reducer
	PointWritable output = new PointWritable();
	PointWritable newCentroid = new PointWritable();

	/**
	 * Combine all points mapped to this centroid in one.
	 */
	public void reduce(PointWritable key, Iterable<PointWritable> values,
			Context context) throws IOException, InterruptedException {

		newCentroid = new PointWritable(key.x, key.y);
		newCentroid.x_sum = key.x_sum;
		newCentroid.y_sum = key.y_sum;
		
		for (PointWritable p : values) {
			newCentroid.addPoint(p);
		}
		context.write(output, newCentroid);
	}
}
