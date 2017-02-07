package Q3;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Report customers whose CountryCode is between 2 and 6 (inclusive).
 * 
 * @author wenlei
 * @author caitlin
 *
 */
public class Query1 {

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

		Job job = Job.getInstance(conf, "Query1");
		job.setJarByClass(Query1.class);
		job.setMapperClass(Q1Mapper.class);
		// job.setReducerClass(Q2Reducer.class);
		// job.setCombinerClass(MRReducer.class);
		// job.setPartitionerClass(MRPartioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {

//		TODO: The assignment doesn't specify, but maybe it would be better to output the entire record, not just the id and name? 
//		probably the primary key would be the first id field
		private Text ID = new Text();
		private Text name = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// customers: id, name, age, countrycode, salary
			String[] tokens = value.toString().split(",");
			int countrycode = Integer.parseInt(tokens[3]);
			if (countrycode >= 2 && countrycode <= 6) {
				ID.set(tokens[3]);
				name.set(tokens[1]);
				context.write(ID, name);
			}
		}
	}

	public static class Q1Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

}