package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PointWritable implements WritableComparable<PointWritable> {

//	id used by centroids
	int id;
	double x;
	double y;
//	count of points when aggregating
	int c;
//	sum of points when aggregating

	public PointWritable() {
	}

	public PointWritable(double x, double y, int c) {
		this.x = x;
		this.y = y;
		this.c = c;
	}

	public PointWritable(String line) {
		String[] split = line.split(",");
		this.x = Double.parseDouble(split[0]);
		this.y = Double.parseDouble(split[1]);
	}
	
	public PointWritable(String line, int c) {
		String[] split = line.split(",");
		this.x = Double.parseDouble(split[0]);
		this.y = Double.parseDouble(split[1]);
		this.c=c;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.x = in.readDouble();
		this.y = in.readDouble();
		this.c = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeDouble(x);
		out.writeDouble(y);
		out.writeInt(c);
	}

	@Override
	public String toString() {
		return x + "," + y;
	}

	public double distTo(PointWritable centroid) {

		return Math.sqrt(Math.pow(this.x - centroid.x, 2)
				+ Math.pow(this.y - centroid.y, 2));
	}

	public void addPoint(PointWritable p) {

		if(this.c == 0){
			this.x = p.x;
			this.y = p.y;
			this.c = p.c;
		}
		else{
			double x = (this.x*c) + (p.x*p.c);
			double y = (this.y*c) + (p.y*p.c);
			this.c += p.c;
			this.x =  (x/this.c);
			this.y = (y/this.c);
		}
	}
	
	

	public void clear() {
		this.x = 0;
		this.y = 0;
		this.c = 0;
	}
	
	@Override
//	based on id, used for centroids only
	public int compareTo(PointWritable p){
		return (this.id - p.id);
	}
	
	public void setId(int id){
		this.id = id;
	}
}
