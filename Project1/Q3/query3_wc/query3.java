package query3;



import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;


import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


public class query3 {
	
	private static String inputC;
	private static String inputT;
	private static String output;

public static void main(String[] args) throws Exception {

	// pass input and output paths as args
	try {
		inputC = args[0];
		inputT = args[1];
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

	Job job = Job.getInstance(conf, "query3");
	job.setJarByClass(query3.class);

	MultipleInputs.addInputPath(job, new Path(inputC),
			TextInputFormat.class, CustomerDataMapper.class);
	MultipleInputs.addInputPath(job, new Path(inputT),
			TextInputFormat.class, TransactionMapper.class);
	
	job.setSortComparatorClass(JoinSortingComparator.class);
	job.setGroupingComparatorClass(JoinGroupingComparator.class);
	
	
	//job.setMapperClass(Q2Mapper.class);
	job.setMapOutputKeyClass(CustomerIdKey.class);
	job.setMapOutputValueClass(JoinGenericWritable.class);
	job.setReducerClass(JoinRecuder.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);
//	job.setCombinerClass(Q2Combiner.class);

	//FileInputFormat.addInputPath(job, new Path(input));
	FileOutputFormat.setOutputPath(job, new Path(output));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
}

	public static class CustomerDataMapper extends Mapper<LongWritable, Text, CustomerIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                           
	        String[] recordFields = value.toString().split(",");
	              
	        int customerId = Integer.parseInt(recordFields[0]);
	        String name = recordFields[1];
	        float salary = Float.parseFloat(recordFields[4]);
	        
	       // int orderQty = Integer.parseInt(recordFields[3]);
	        //double lineTotal = Double.parseDouble(recordFields[8]);
	                                               
	        CustomerIdKey recordKey = new CustomerIdKey(customerId, CustomerIdKey.Customer_RECORD);
	        CustomerRecord record = new CustomerRecord(name,salary);
	                                               
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	
	public static class TransactionMapper extends Mapper<LongWritable, Text, CustomerIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] recordFields = value.toString().split(",");
	        int customerId = Integer.parseInt(recordFields[1]);
	        int transactionid = 1;  // give everytransaction one, use for calculation later
	        double transTotal = Double.parseDouble(recordFields[2]);
	        int transNumItems = Integer.parseInt(recordFields[3]);
	        
	        //String productName = recordFields[1];
	        //String productNumber = recordFields[2];
	                                               
	        CustomerIdKey recordKey = new CustomerIdKey(customerId, CustomerIdKey.DATA_RECORD);
	        TransactionRecord record = new TransactionRecord (transactionid, transTotal,transNumItems );
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	
	
	public static class JoinRecuder extends Reducer<CustomerIdKey, JoinGenericWritable, NullWritable, Text>{
	    public void reduce(CustomerIdKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
	        StringBuilder output = new StringBuilder();
	        int transcount = 0;  //counter
	        int minTransItem = Integer.MAX_VALUE;
	        double sumTrnsTotal = 0.0;
	                                               
	        for (JoinGenericWritable v : values) {
	            Writable record = v.get();
	            if (key.recordType.equals(CustomerIdKey.Customer_RECORD)){
	                CustomerRecord cRecord = (CustomerRecord)record;
	                output.append(Integer.parseInt(key.customerId.toString())).append(", ");
	                output.append(cRecord.name.toString()).append(", ");
	                output.append(cRecord.salary.toString()).append(", ");
	            } else {
	            	TransactionRecord record2 = (TransactionRecord)record;
	            	
	            	transcount += Integer.parseInt(record2.transID.toString());
;
	            	sumTrnsTotal  += Double.parseDouble(record2.transtotal.toString());
	            	minTransItem =Math.min(minTransItem,Integer.parseInt(record2.transNumItems.toString()));  //need to convert to both
	            			
	            }
	        }
	        // remove filter
	        //if (sumOrderQty > 0) {
	            context.write(NullWritable.get(), new Text(output.toString() + transcount + ", " + sumTrnsTotal + ", " + minTransItem));
	        //}
	    }
	}
	
	
	
	public static class JoinGroupingComparator extends WritableComparator {
	   
		public JoinGroupingComparator() {
	        super (CustomerIdKey.class, true);
	    }                             

	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	    	CustomerIdKey first = (CustomerIdKey) a;
	    	CustomerIdKey second = (CustomerIdKey) b;
	                      
	        return first.customerId.compareTo(second.customerId);
	    }
	   
	    
	}
	
	
	public static class JoinSortingComparator extends WritableComparator {
	    public JoinSortingComparator()
	    {
	        super (CustomerIdKey.class, true);
	    }
	                               
	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	    	CustomerIdKey first = (CustomerIdKey) a;
	    	CustomerIdKey second = (CustomerIdKey) b;
	                                 
	        return first.compareTo(second);
	    }
	}
	
}

