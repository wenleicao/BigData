package Q2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Q2 {

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
		job.setJarByClass(Q2.class);
		job.setInputFormatClass(JsonInputFormat.class);
		job.setMapperClass(Q2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(Q2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//job.setCombinerClass(Q2Combiner.class);

		FileInputFormat.addInputPath(job, new Path(input));
				
		//set file input split size 5* default block size (64M) 
		FileInputFormat.setMinInputSplitSize(job, 5*67108864l);
		
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

		//private Text result = new Text();
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
		public void reduce(Text key,
				Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int numCount = 0;
			

			for (IntWritable trans : values) {
			numCount +=  Integer.parseInt(trans.toString());
			}
			totalcount.set(numCount);
			
			//result.set(key.toString() + "," + numTrans + "," + totalSum);
			context.write(key, totalcount);
		}
	}

	
	}




