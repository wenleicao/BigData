package query3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class CustomerIdKey implements WritableComparable<CustomerIdKey> {
	
	public IntWritable customerId = new IntWritable();
	public IntWritable recordType = new IntWritable();
	
	public CustomerIdKey(){}
	public CustomerIdKey(int customerId, IntWritable recordType) {
	    this.customerId.set(customerId);
	    this.recordType = recordType;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.customerId.readFields(in);
	    this.recordType.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		this.customerId.write(out);
		this.recordType.write(out);
		
	}
	@Override
	public int compareTo(CustomerIdKey other) {
		  if (this.customerId.equals(other.customerId )) {
		        return this.recordType.compareTo(other.recordType);
		    } else {
		        return this.customerId.compareTo(other.customerId);
		    }
	}
	
	public boolean equals (CustomerIdKey other) {
	    return this.customerId.equals(other.customerId) && this.recordType.equals(other.recordType );
	}

	public int hashCode() {
	    return this.customerId.hashCode();
	}
	
	public static final IntWritable Customer_RECORD = new IntWritable(0);
	public static final IntWritable DATA_RECORD = new IntWritable(1);
}
