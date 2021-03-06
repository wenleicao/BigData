package json;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q2 {

	private static String input;
	private static String output;

	public static void main(String[] args) throws Exception {
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
		
		// use custom input format
		job.setInputFormatClass(JsonInputFormat.class);
		job.setMapperClass(MRMapper.class);
		job.setReducerClass(MRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));

		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MRMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text line = new Text();
		private IntWritable count = new IntWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			// get flag block
			String flag = tokens[5];
			// extract flag number
			String flagnumber = flag.substring(7, flag.length());
			count.set(1);
			line.set(flagnumber);
			context.write(line, count);
		}
	}

	public static class MRReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum); // output count
			context.write(key, result);
		}
	}

}
