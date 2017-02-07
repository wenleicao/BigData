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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Q3.Query2.Q2Reducer;

/**
 * Join the Customers and Transactions datasets. output: CustomerID, Name,
 * Salary, NumOfTransactions, TotalSum, MinItems NumOfTransactions is the total
 * number of transactions done by the customer TotalSum is the sum of field
 * “TransTotal” for that customer MinItems is the minimum number of items in
 * transactions done by the customer.
 * 
 * @author caitlin
 *
 */
public class Query3 {

	private static String input_cust;
	private static String input_trans;
	private static String output;

	public static void main(String[] args) throws Exception {

		// pass input and output paths as args
		try {

			input_cust = args[0];
			input_trans = args[1];
			output = args[2];
		} 
		catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		Configuration conf = new Configuration();
		
		// delete old output directories if they exist
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}

		Job job = Job.getInstance(conf, "Query3");
		job.setJarByClass(Query3.class);

		MultipleInputs.addInputPath(job, new Path(input_cust),
				TextInputFormat.class, Q3CustomerMapper.class);
		MultipleInputs.addInputPath(job, new Path(input_trans),
				TextInputFormat.class, Q3TransactionMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TransCountWritable.class);

		job.setReducerClass(Q3Reducer.class);
//		job.setCombinerClass(Q3Combiner.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	
	public static class Q3CustomerMapper extends
			Mapper<LongWritable, Text, IntWritable, TransCountWritable> {

		private IntWritable id_key = new IntWritable();
		private TransCountWritable trans = new TransCountWritable();

		/**
		 * Map customers by id.
		 *
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// customers: id, name, age, countrycode, salary
			String[] tokens = value.toString().split(",");
			int id = Integer.parseInt(tokens[0]);
			String name = tokens[1];
			float salary = Float.parseFloat(tokens[4]);
			id_key.set(id);
			trans.setId(id);
			trans.setName(name);
			trans.setSalary(salary);
			context.write(id_key, trans);
		}
	}

	public static class Q3TransactionMapper extends
			Mapper<LongWritable, Text, IntWritable, TransCountWritable> {

		private IntWritable id_key = new IntWritable();
		private TransCountWritable trans = new TransCountWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// transactions: transId, custId, transTotal,transNumItems,transDesc
			String[] tokens = value.toString().split(",");
			int customerId = Integer.parseInt(tokens[1]);
			id_key.set(customerId);
			trans.setId(customerId);

			float transTotal = Float.parseFloat(tokens[2]);
			int transNumItems = Integer.parseInt(tokens[3]);
			trans.addTrans(transTotal, transNumItems);
			context.write(id_key, trans);
		}
	}

	public static class Q3Reducer extends
			Reducer<IntWritable, TransCountWritable, NullWritable, Text> {
		
		// output: CustomerID, Name, Salary, NumOfTransactions, TotalSum,MinItems
		
		// NumOfTransactions is the total number of transactions done by the customer
		// TotalSum is the sum of field “TransTotal” for that customer
		// MinItems is the minimum number of items in transactions done by the customer.

		private Text result = new Text();
		
		/**
		 * Join data nd aggregate results.
		 *
		 */
		public void reduce(IntWritable key,
				Iterable<TransCountWritable> values, Context context)
				throws IOException, InterruptedException {

			String custInfo = null;
			int numTrans = 0;
			float totalSum = 0;
			int minItems = Integer.MAX_VALUE;

			for (TransCountWritable trans : values) {
				
//				if this trans contains customer info
				if(trans.getCount() == 0){
					custInfo = trans.toCustomerString();
				}
				else{
//					trans info
					numTrans += trans.getCount();
					totalSum += trans.getTotal();
					minItems = Math.min(minItems, trans.getMinItems());
				}
				
			}
			result.set(custInfo+","+numTrans+","+totalSum+","+minItems);
			context.write(NullWritable.get(), result);
		}
	}

	public static class Q3Combiner extends
			Reducer<IntWritable, TransCountWritable, IntWritable, TransCountWritable> {

		private TransCountWritable result = new TransCountWritable();

		// output format: CustomerID, NumTransactions, TotalSum
		public void reduce(IntWritable key,
				Iterable<TransCountWritable> values, Context context)
				throws IOException, InterruptedException {

			int numTrans = 0;
			float totalSum = 0;
			int minItems = Integer.MAX_VALUE;
			result.clear();
			result.setId(key.get());

			for (TransCountWritable trans : values) {
				
//				if this trans contains customer info
				if(trans.getName() != null){
					result.setName(trans.getName());
					result.setSalary(trans.getSalary());
				}
				else{
//					trans info
					numTrans += trans.getCount();
					totalSum += trans.getTotal();
					minItems = Math.min(minItems, trans.getMinItems());
				}
				
			}
			result.setCount(numTrans);
			result.setTotal(totalSum);
			result.setMinItems(minItems);
			context.write(key, result);
		}
	}

}