package Q3.query4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class TransactionRecord implements Writable {
	    public IntWritable transID = new IntWritable();
	    public DoubleWritable transtotal = new DoubleWritable(); 
	    public IntWritable transNumItems = new IntWritable();

	    public TransactionRecord(){}              

	    public TransactionRecord(int transID, double transtotal, int transNumItems) {
	        this.transID.set(transID);
	        this.transtotal.set(transtotal);
	        this.transNumItems.set(transNumItems);
	    }
	    
	    public TransactionRecord(double transtotal) {
	          this.transtotal.set(transtotal);
	     	    }


	 
		@Override
		public void readFields(DataInput in) throws IOException {
			this.transID.readFields(in);
	        this.transtotal.readFields(in);
	        this.transNumItems.readFields(in);
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.transID.write(out);
	        this.transtotal.write(out);
	        this.transNumItems.write(out);
	        
			
		}
	}