package kmeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCombiner extends
		Reducer<PointWritable, PointWritable, PointWritable, PointWritable> {

//	send all data to one reducer
	PointWritable newPoint = new PointWritable();

	/**
	 * Combine all points mapped to this centroid in one.
	 */
	public void reduce(PointWritable key, Iterable<PointWritable> values,
			Context context) throws IOException, InterruptedException {

		newPoint.clear();
		
		for (PointWritable p : values) {
			newPoint.addPoint(p);
		}
		context.write(key, newPoint);
	}
}
