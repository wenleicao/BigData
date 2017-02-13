package Q3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class Trans4Writable implements Writable{

	private float minTotal;
	private float maxTotal;
	private Set<Integer> customers;
	
	public Trans4Writable() {
		this.minTotal = Float.MAX_VALUE;
	}
	
	public Trans4Writable(int id, float total){
		this.minTotal = total;
		this.maxTotal = total;
		this.customers = new HashSet<Integer>();
		customers.add(id);
	}

	public void clear(){
		this.minTotal = Float.MAX_VALUE;
		this.maxTotal = 0;
		this.customers.clear();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		minTotal = in.readFloat();
		maxTotal = in.readFloat();
		int numCustomers = in.readInt();
		customers = new HashSet<Integer>(numCustomers);
		for (int i=0 ; i<numCustomers ; i++){
			customers.add(in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeFloat(minTotal);
		out.writeFloat(maxTotal);
		out.writeInt(customers.size());
		for (Integer i : customers){
			out.writeInt(i.intValue());
		}
	}
	
	/**Add customer id (only if not already seen).**/
	public void addCustomer(int id){
		if(!customers.contains(id)){
			customers.add(id);
		}
	}
	
	/**Update min and max total values.**/
	public void addTrans(float total){
		minTotal = Math.min(minTotal, total);
		maxTotal = Math.max(maxTotal, total);
	}
	
	public int getNumCustomers(){
		return customers.size();
	}

	public void addCustomers(Collection<Integer> customers) {
		this.customers.addAll(customers);
		
	}
	
	public float getMinTotal() {
		return minTotal;
	}

	public void setMinTotal(float minTotal) {
		this.minTotal = minTotal;
	}

	public float getMaxTotal() {
		return maxTotal;
	}

	public void setMaxTotal(float maxTotal) {
		this.maxTotal = maxTotal;
	}

	public Collection<Integer> getCustomers() {
		return customers;
	}

}
