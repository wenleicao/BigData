package query4b;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query4b {
	
	private static String input;
	private static String output;
	
	  public static void main(String[] args) throws Exception {
		  try{
			  input = args[0];
			  output = args[1];
		  }
		  catch(Exception e){
			  e.printStackTrace();
			  System.exit(0);
		  }
		  
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Query4b");
		    job.setJarByClass(Query4b.class);
		    job.setMapperClass(MRMapper.class);
		    job.setReducerClass(MRReducer.class);
//		    job.setCombinerClass(MRReducer.class);
//		    job.setPartitionerClass(MRPartioner.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(input));
		    FileOutputFormat.setOutputPath(job, new Path(output));

		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

  public static class MRMapper 
       extends Mapper<Object, Text, IntWritable, Text>{
    
		private IntWritable countrycode = new IntWritable();
		private Text measures = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(", ");
			StringBuilder output = new StringBuilder();
			int id = Integer.parseInt(tokens[0]);
			float transcount = Float.parseFloat(tokens[1]);
			double minTransTotal = Double.parseDouble(tokens[2]);
			double maxTransTotal = Double.parseDouble(tokens[3]);
			output.append(transcount).append(",");
			output.append(minTransTotal).append(",");
			output.append(maxTransTotal);
			
			countrycode.set(id);
			measures.set(output.toString());
			
			context.write(countrycode, measures);
      
    }
  }
  
  public static class MRReducer 
       extends Reducer<IntWritable,Text,IntWritable,Text > {
    //private IntWritable countrycode = new IntWritable();
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      float transcount = 0;
      double minTransTotal = Double.MAX_VALUE;
      double maxTransTotal = Double.MIN_VALUE;
      StringBuilder output = new StringBuilder();
      
      for (Text val : values) {
        String [] tokens = val.toString().split(",");
    	transcount += Float.parseFloat(tokens[0]);
    	minTransTotal =Math.min(minTransTotal,Double.parseDouble(tokens[1]));
    	maxTransTotal =Math.max(maxTransTotal,Double.parseDouble(tokens[2]));  
      }
      output.append((int)transcount).append(", ").append(minTransTotal).append(", ").append(maxTransTotal); 
      result.set(output.toString());
      context.write(key, result);
    }
  }


}