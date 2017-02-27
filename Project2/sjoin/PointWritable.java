package sjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 2d point object.
 * @author caitlin
 *
 */
public class PointWritable implements WritableComparable<PointWritable> {

	int x;
	int y;

	public PointWritable() {
	}

	public PointWritable(int x, int y) {
		this.x = x;
		this.y = y;
	}

	public PointWritable(int x, int y, int c) {
		this.x = x;
		this.y = y;
	}

	public PointWritable(String line) {
		String[] split = line.split(",");
		this.x = Integer.parseInt(split[0]);
		this.y = Integer.parseInt(split[1]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readInt();
		y = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(x);
		out.writeInt(y);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("(");
		sb.append(x);
		sb.append(",");
		sb.append(y);
		sb.append(")");
		return sb.toString();
	}
	
	@Override
//	returns 0 if x and y are the same
	public int compareTo(PointWritable p){
		return (this.x-p.x)+(this.y-p.y);
	}
}
