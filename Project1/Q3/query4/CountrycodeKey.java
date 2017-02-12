package query4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class CountrycodeKey implements WritableComparable<CountrycodeKey> {
	
	public IntWritable countrycode = new IntWritable();
	
	
	public CountrycodeKey(){}
	public CountrycodeKey(int countrycode) {
	    this.countrycode.set(countrycode);
	    
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.countrycode.readFields(in);
	  
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		this.countrycode.write(out);
			
	}
	
	
	public boolean equals (CountrycodeKey other) {
	    return this.countrycode.equals(other.countrycode);
	}

	
	@Override
	public int compareTo(CountrycodeKey o) {
		if (this.countrycode.equals(o.countrycode )) {
	        return 1;
	    } else {
	    	return 0;
	    }
		
	}
}
