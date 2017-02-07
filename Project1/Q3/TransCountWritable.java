package Q3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TransCountWritable implements WritableComparable<TransCountWritable>{

	private int customerId;
	private String name;
	private float salary;
	private int count;
	private float total;
	private int minItems;
	
	public TransCountWritable() {}
	
	public TransCountWritable(int customerId) {
		this.customerId = customerId;
		this.minItems = Integer.MAX_VALUE;
	}
	
	public TransCountWritable(int count, float total){
		this.count = count;
		this.total = total;
		this.minItems = Integer.MAX_VALUE;
	}

	public void clear(){
		this.customerId = 0;
		this.name = null;
		this.salary = 0f;
		this.count = 0;
		this.total = 0f;
		this.minItems = Integer.MAX_VALUE;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		customerId = in.readInt();
		name = in.readLine();
		salary = in.readFloat();
		count = in.readInt();
		total = in.readFloat();
		minItems = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(customerId);
		if(name != null){
			out.writeChars(name);
		}
		out.writeChar('\n');
		out.writeFloat(salary);
		out.writeInt(count);
		out.writeFloat(total);
		out.writeInt(minItems);
	}
	
	public void addTrans(float total){
		count++;
		this.total += total;
	}
	
	public void addTrans(float total, int numItems){
		this.count++;
		this.total += total;
		if (minItems > numItems){
			minItems = numItems;
		}
	}
	
	public int getId(){
		return customerId;
	}
	
	public void setId(int customerId) {
		this.customerId = customerId;
	}
	
	public int getCount(){
		return count;
	}
	
	public void setCount(int count){
		this.count=count;
	}
	
	public float getTotal(){
		return total;
	}
	
	public void setTotal(float total){
		this.total =total;
	}
	
	public int getMinItems(){
		return minItems;
	}
	
	public void setMinItems(int minItems){
		this.minItems = minItems;
	}
	
	public float getSalary() {
		return salary;
	}

	public void setSalary(float salary) {
		this.salary = salary;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String toCustomerString(){
		// output: CustomerID, Name, Salary
		return customerId+","+name+","+salary;
	}
	
	@Override
	public int compareTo(TransCountWritable t) {
		return customerId - t.getId();
	}

	

	
	
}
