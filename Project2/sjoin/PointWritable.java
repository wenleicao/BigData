

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PointWritable implements WritableComparable<PointWritable> {

	int x;
	int y;
	int c;
	int x_sum;
	int y_sum;

	public PointWritable() {
	}

	public PointWritable(int x, int y) {
		this.x = x;
		this.y = y;
	}

	public PointWritable(int x, int y, int c) {
		this.x = x;
		this.y = y;
		this.c = c;
	}

	public PointWritable(String line) {
		String[] split = line.split(",");
		this.x = Integer.parseInt(split[0]);
		this.y = Integer.parseInt(split[1]);
	}
	
	public PointWritable(String line, int c) {
		String[] split = line.split(",");
		this.x = Integer.parseInt(split[0]);
		this.y = Integer.parseInt(split[1]);
		this.c=c;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readInt();
		y = in.readInt();
		c = in.readInt();
		x_sum = in.readInt();
		y_sum = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(x);
		out.writeInt(y);
		out.writeInt(c);
		out.writeInt(x_sum);
		out.writeInt(y_sum);
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

		this.x_sum += p.x;
		this.y_sum += p.y;
		this.c +=p.c;
	}
	
	public void addPoints(int x_sum, int y_sum, int c) {

		this.x_sum += x_sum;
		this.y_sum += y_sum;
		this.c +=c;
	}

	public void clear() {
		this.x = 0;
		this.y = 0;
		this.c = 0;
	}

	public void update() {

		if (c == 0) {
			this.clear();
		} else {
			this.x = this.x_sum / c;
			this.y = this.y_sum / c;
		}
	}
	
	@Override
//	returns 0 if x and y are the same
	public int compareTo(PointWritable p){
		return (this.x-p.x)+(this.y-p.y);
	}
}
