package Q3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
 * Reports for every country code, the number of customers having this code as
 * well as the min and max of TransTotal fields for the transactions done by
 * those customers. output format: CountryCode, NumberOfCustomers,
 * MinTransTotal, MaxTransTotal
 * 
 * @author caitlin
 * 
 */
public class Query4 {

	private static String input_cust;
	private static String input_trans;
	private static String output;

	public static void main(String[] args) throws Exception {

		// pass input and output paths as args
		try {

			input_cust = args[0];
			input_trans = args[1];
			output = args[2];
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
		// save path to customer file in conf
		conf.set("customers", input_cust);

		Job job = Job.getInstance(conf, "Query3");
		job.setJarByClass(Query4.class);
		FileInputFormat.addInputPath(job, new Path(input_trans));
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Trans4Writable.class);

		job.setMapperClass(Q4TransactionMapper.class);
		job.setReducerClass(Q4Reducer.class);
		job.setCombinerClass(Q4Combiner.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Q4TransactionMapper extends
			Mapper<LongWritable, Text, IntWritable, Trans4Writable> {

		/**
		 * Table to store customer data.
		 */
		private static HashMap<Integer, Integer> customers;
		private static IntWritable cc_key = new IntWritable();
		private static Trans4Writable trans = new Trans4Writable();

		/**
		 * Load customer data into a hashtable, map ids to country codes.
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			String cname = conf.get("customers");
			customers = new HashMap<Integer, Integer>();
			FileSystem fs = FileSystem.get(conf);
			BufferedReader fis = null;
			FSDataInputStream in = fs.open(new Path(cname));
			try {
				fis = new BufferedReader(new InputStreamReader(in));
				String line;
				while ((line = fis.readLine()) != null) {
					String[] tokens = line.split(",");

					// customers: id, name, age, countrycode, salary
					int id = Integer.parseInt(tokens[0]);
					int cc = Integer.parseInt(tokens[3]);
					customers.put(id, cc);
				}
			} catch (IOException ioe) {
				ioe.printStackTrace();

			} finally {
				if (in != null) {
					fis.close();
					in.close();
				}
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// transactions: transId, custId,
			// transTotal,transNumItems,transDesc
			String[] tokens = value.toString().split(",");

			// get countrycode for customer
			int customerId = Integer.parseInt(tokens[1]);
			int countryCode = customers.get(customerId);
			float total = Float.parseFloat(tokens[2]);
			cc_key.set(countryCode);
			trans = new Trans4Writable(customerId, total);

			context.write(cc_key, trans);
		}
	}

	public static class Q4Reducer extends
			Reducer<IntWritable, Trans4Writable, NullWritable, Text> {

		private static Text result = new Text();

		/**
		 * Join data and aggregate results.
		 * 
		 */
		public void reduce(IntWritable key, Iterable<Trans4Writable> values,
				Context context) throws IOException, InterruptedException {

			// output format: CountryCode, NumberOfCustomers, MinTransTotal,
			// MaxTransTotal
			StringBuffer sb = new StringBuffer(key.toString());
			sb.append(",");

			float minTrans = Float.MAX_VALUE;
			float maxTrans = 0f;
			int numCust = 0;

			for (Trans4Writable trans : values) {

				minTrans = Math.min(minTrans, trans.getMinTotal());
				maxTrans = Math.max(maxTrans, trans.getMaxTotal());
				numCust += trans.getNumCustomers();
			}

			sb.append(numCust);
			sb.append(",");
			sb.append(minTrans);
			sb.append(maxTrans);
			result.set(sb.toString());

			context.write(NullWritable.get(), result);
		}
	}

	public static class Q4Combiner extends
			Reducer<IntWritable, Trans4Writable, IntWritable, Trans4Writable> {

		private Trans4Writable result = new Trans4Writable();

		public void reduce(IntWritable key, Iterable<Trans4Writable> values,
				Context context) throws IOException, InterruptedException {

			result.clear();

			for (Trans4Writable trans : values) {
				// update total - all output from mappers have same val for min
				// and max
				result.addTrans(trans.getMaxTotal());
				result.addCustomers(trans.getCustomers());
			}
			context.write(key, result);
		}
	}

}