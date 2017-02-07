package Q3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Report for every customer the number of transactions and the total sum of
 * their transactions. output format: CustomerID, NumTransactions, TotalSum (You
 * are required to use a Combiner in this query.)
 * 
 * @author caitlin
 *
 */
public class Query2 {

	private static String input;
	private static String output;

	public static void main(String[] args) throws Exception {

		// pass input and output paths as args
		try {
			input = args[0];
			output = args[1];
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
		job.setJarByClass(Query2.class);
		job.setMapperClass(Q2Mapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TransCountWritable.class);

		job.setReducerClass(Q2Reducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setCombinerClass(Q2Combiner.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Q2Mapper extends
			Mapper<LongWritable, Text, IntWritable, TransCountWritable> {

		private IntWritable id_key = new IntWritable();
		private TransCountWritable tcount = new TransCountWritable();

		/**
		 * Partition transaction file by customer id.
		 *
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// transactions: transId, custId, transTotal,
			// transNumItems,transDesc

			String[] tokens = value.toString().split(",");
			int id = Integer.parseInt(tokens[1]);
			id_key.set(id);
			//
			float transTotal = Float.parseFloat(tokens[2]);

			tcount.clear();
			tcount.addTrans(transTotal);
			context.write(id_key, tcount);
		}
	}

	public static class Q2Reducer extends
			Reducer<IntWritable, TransCountWritable, NullWritable, Text> {

		private Text result = new Text();

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
		public void reduce(IntWritable key,
				Iterable<TransCountWritable> values, Context context)
				throws IOException, InterruptedException {

			int numTrans = 0;
			float totalSum = 0;

			for (TransCountWritable trans : values) {
				numTrans += trans.getCount();
				totalSum += trans.getTotal();
			}
			result.set(key.toString() + "," + numTrans + "," + totalSum);
			context.write(NullWritable.get(), result);
		}
	}

	public static class Q2Combiner
			extends
			Reducer<IntWritable, TransCountWritable, IntWritable, TransCountWritable> {

		private TransCountWritable result = new TransCountWritable();

		/**
		 * Aggregate transactions per customer.
		 * 
		 */
		// output format: CustomerID, NumTransactions, TotalSum
		public void reduce(IntWritable key,
				Iterable<TransCountWritable> values, Context context)
				throws IOException, InterruptedException {

			int numTrans = 0;
			float totalSum = 0;

			for (TransCountWritable trans : values) {
				numTrans += trans.getCount();
				totalSum += trans.getTotal();
			}
			result.setCount(numTrans);
			result.setTotal(totalSum);
			context.write(key, result);
		}
	}

}