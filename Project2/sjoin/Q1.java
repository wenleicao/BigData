package sjoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {

	private static String input_points;
	private static String input_rec;
	private static String output;
	private static int[] window;

	public static void main(String[] args) throws Exception {

		// pass input and output paths as args
		try {
			input_points = args[0];
			input_rec = args[1];
			output = args[2];
			
//			optional window param
//			format:W(x1,y1,x2,y2)	
			
			if(args.length > 3){
				window = new int[4];
				String win = (String) args[3].subSequence(2, args[3].length()-2);
				String[] split = win.split(",");
				for(int i=0 ; i<split.length ; i++){
					window[i]=Integer.parseInt(split[i]);
				}
			}
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
		job.setMapperClass(Q2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(Q2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Q2Mapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text id_key = new Text();
		private IntWritable tcount = new IntWritable();

		/**
		 * Partition transaction file by customer id.
		 *
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// transactions: transId, custId, transTotal,
			// transNumItems,transDesc

			String[] tokens = value.toString().split(",");
			String id = tokens[5];
			id_key.set(id);

			tcount.set(1);
			context.write(id_key, tcount);
		}
	}

	public static class Q2Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		// private Text result = new Text();
		private IntWritable totalcount = new IntWritable();

		/**
		 * Aggregate transactions per customer.
		 * 
		 * @param key
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		// output format: CustomerID, NumTransactions, TotalSum
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int numCount = 0;

			for (IntWritable trans : values) {
				numCount += Integer.parseInt(trans.toString());
			}
			totalcount.set(numCount);

			// result.set(key.toString() + "," + numTrans + "," + totalSum);
			context.write(key, totalcount);
		}
	}

}
