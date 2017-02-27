package kmeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends
		Reducer<PointWritable, PointWritable, Text, NullWritable> {

	PointWritable point = new PointWritable();
	Text output = new Text();
	static int k;

	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		k = conf.getInt("k", 10);
	}

	/**
	 * compute new centroid.
	 */
	public void reduce(PointWritable key, Iterable<PointWritable> values,
			Context context) throws IOException, InterruptedException {

		ArrayList<PointWritable> centroids = new ArrayList<PointWritable>();

		for (PointWritable p : values) {
			
			if(centroids.contains(p)){
				PointWritable pw = centroids.get(centroids.indexOf(p));
				centroids.remove(p);
				pw.addPoints(p.x_sum, p.y_sum,p.c);
				centroids.add(pw);
			}
			else{
				centroids.add(p);
			}
		}
		
		for (PointWritable p : centroids){
			point.update();
			output.set(point.toString());
			context.write(output, NullWritable.get());
		}
	}
}
