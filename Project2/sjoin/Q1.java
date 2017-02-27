package sjoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Perform spatial join between input sets of points and rectangles.
 * 
 * @author caitlin
 *
 */
public class Q1 {

	private static String input_points;
	private static String input_rectangles;
	private static String output;
	private static RectangleWritable window;

	private static int GRID_SIZE;

	/**
	 * 
	 * @param args
	 *            : input points, input rectangles, optional window param
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// parse args, initialize values
		try {
			input_points = args[0];
			input_rectangles = args[1];
			output = args[2];

			// optional window param
			// format:W(x1,y1,x2,y2)

			if (args.length > 3) {
				String win = (String) args[3].subSequence(2,
						args[3].length() - 1);
				String[] split = win.split(",");

//				define window of interest id = -1
				window = new RectangleWritable(-1, Integer.parseInt(split[0]),
						Integer.parseInt(split[1]), Integer.parseInt(split[2]),
						Integer.parseInt(split[3]));
			} 
			else {
//				default is entire domain space
				window = new RectangleWritable(-1, 0, 0, 10000, 10000);
			}
			
//			set grid size between 100 and 1000
			GRID_SIZE = Math.min(window.w/100, window.h/100);
			GRID_SIZE = Math.min(GRID_SIZE, 100); 
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		Configuration conf = new Configuration();

		// delete old output directories if they exist
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}

		Job job = Job.getInstance(conf, "Query2");
		job.setJarByClass(Q1.class);
		job.setInputFormatClass(TextInputFormat.class);
		
//		mappers
		MultipleInputs.addInputPath(job, new Path(input_points),
				TextInputFormat.class, PointMapper.class);
		MultipleInputs.addInputPath(job, new Path(input_rectangles),
				TextInputFormat.class, RectangleMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Geometry.class);
		
//		reducers
		job.setReducerClass(Q1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Geometry.class);

		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Find the x index of a grid cell.
	 * 
	 * @param x
	 * @return
	 */
	public static int getIndexX(int x) {
		return (x - window.x) / GRID_SIZE;
	}

	/**
	 * Find the y index of a grid cell.
	 * 
	 * @param y
	 * @return
	 */
	public static int getIndexY(int y) {
		return (y - window.y) / GRID_SIZE;
	}

	/**
	 * hash a point into a grid cell.
	 * 
	 * @param min
	 * @param max
	 * @param numBuckets
	 * @return index number
	 */
	public static int gridHash(int x, int y) {

		// compute grid index (uses Cantor's enumeration of pairs).
		int n = ((x + y) * (x + y + 1) / 2) + y;
		return n;
	}

	/**
	 * Hash a rectangle to a set of grid indices.
	 * 
	 * @param min
	 * @param max
	 * @param numBuckets
	 * @return matrix of indices
	 */
	public static int[][] gridHash(RectangleWritable r) {

		// get range of indices for each dimension
		int start_x = getIndexX(r.x);
		int end_x = getIndexX(r.x + r.w);
		int start_y = getIndexX(r.y);
		int end_y = getIndexX(r.y + r.h);

		int range_x = end_x - start_x + 1;
		int range_y = end_y - start_y + 1;
		int[][] xy_indices = new int[range_x][range_y];

//		build matrix index values
		for (int i = 0; i < range_x; i++) {
			for (int j = 0; j < range_y; j++) {
				xy_indices[i][j] = gridHash(start_x + i, start_y + j);
			}
		}
		return xy_indices;

	}
	/**
	 * Map points to grid cells.
	 *
	 */
	public static class PointMapper extends
			Mapper<LongWritable, Text, IntWritable, Geometry> {

		private static IntWritable index = new IntWritable();
		private static Geometry g = new Geometry();
		private static PointWritable point = new PointWritable();


		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			point = new PointWritable(value.toString());

			// check if point falls in window
			if (window.contains(point)) {
				// find x,y indices
				int x_grid = getIndexX(point.x);
				int y_grid = getIndexY(point.y);

				index.set(gridHash(x_grid, y_grid));
				g = new Geometry(point);
				context.write(index, g);
			}
		}
	}

	/**
	 * Map rectangles to grid cells.
	 * @author caitlin
	 *
	 */
	public static class RectangleMapper extends
			Mapper<LongWritable, Text, IntWritable, Geometry> {

		private IntWritable index = new IntWritable();
		private static Geometry g = new Geometry();
		private RectangleWritable rectangle = new RectangleWritable();

		
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			rectangle = new RectangleWritable(value.toString());

			// check if rectangle overlaps window
			if (window.overlaps(rectangle)) {
				int[][] indices = gridHash(rectangle);

				for (int i = 0; i < indices.length; i++) {
					for (int j = 0; j < indices[0].length; j++) {
						index.set(indices[i][j]);
						g = new Geometry(rectangle);
						context.write(index, g);
					}
				}

			}
		}
	}

	public static class Q1Reducer extends
			Reducer<IntWritable, Geometry, Text, NullWritable> {

		private Text result = new Text();

		/**
		 * Join points and rectangles within one grid cell. output format:
		 * <r1,(x,y)>
		 * 
		 * @param key
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		// output format: CustomerID, NumTransactions, TotalSum
		public void reduce(IntWritable key, Iterable<Geometry> values,
				Context context) throws IOException, InterruptedException {

			ArrayList<PointWritable> points = new ArrayList<PointWritable>();
			ArrayList<RectangleWritable> rectangles = new ArrayList<RectangleWritable>();
			
			for (Geometry g : values) {
				Writable rawValue = g.get();
				if (rawValue instanceof RectangleWritable) {
					rectangles.add((RectangleWritable) rawValue);
				} else {
					points.add((PointWritable) rawValue);
				}
			}
			for (PointWritable p : points) {
				for (RectangleWritable r : rectangles) {
					if (r.contains(p)) {
						result.set("<"+r.toString() + "," + p.toString()+">");
						context.write(result, NullWritable.get());
					}
				}
			}
		}
	}
}
