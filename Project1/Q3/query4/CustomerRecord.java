package query4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CustomerRecord implements Writable {

    public Text name = new Text();
    public FloatWritable salary = new FloatWritable();
    public IntWritable countrycode = new IntWritable();
    

    public CustomerRecord(){}
               
    public CustomerRecord(String Name, float Salary){
        this.name.set(Name);
        this.salary.set(Salary);
       }

    public CustomerRecord(int CountryCode){
         this.countrycode.set(CountryCode);
       }
    
    public CustomerRecord(float Salary, int CountryCode){
        this.countrycode.set(CountryCode);
        this.salary.set(Salary);
      }

	@Override
	public void readFields(DataInput in) throws IOException {
		 this.name.readFields(in);
	        this.salary.readFields(in);
	        this.countrycode.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		 this.name.write(out);
	        this.salary.write(out);
	        this.countrycode.write(out);
	        
		
	}
}