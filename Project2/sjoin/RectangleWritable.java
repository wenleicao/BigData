package sjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Rectangle class extends PointWritable, adding height and width information.
 * 
 * @author caitlin
 *
 */
public class RectangleWritable extends PointWritable {

	int id;
	int h;
	int w;

	public RectangleWritable() {
	}

	public RectangleWritable(int id, int x, int y, int h, int w) {
		this.id = id;
		this.x = x;
		this.y = y;
		this.h = h;
		this.w = w;
	}

	/**
	 * Parse input string of format "id,x,y,h,w".
	 * 
	 * @param line
	 */
	public RectangleWritable(String line) {
		String[] split = line.split(",");
		this.id = Integer.parseInt(split[0]);
		this.x = Integer.parseInt(split[1]);
		this.y = Integer.parseInt(split[2]);
		this.h = Integer.parseInt(split[3]);
		this.w = Integer.parseInt(split[4]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		x = in.readInt();
		y = in.readInt();
		h = in.readInt();
		w = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(x);
		out.writeInt(y);
		out.writeInt(h);
		out.writeInt(w);
	}

	@Override
	public String toString() {
		return String.valueOf(id);
	}

	/**
	 * find if a point falls within this rectangle
	 * 
	 * @param p
	 *            2d point
	 * @return
	 */
	public boolean contains(PointWritable p) {

		return this.x <= p.x && p.x < this.x + this.w && this.y <= p.y
				&& p.y < this.y + this.h;
	}

	/**
	 * find if this rectangle overlaps with other rectangle
	 * 
	 * @param r
	 *            rectangle
	 * @return
	 */
	public boolean overlaps(RectangleWritable r) {

		// is this rectangle completely left or completely right the other?
		if (this.x + this.w < r.x || this.x > r.x + r.w) {
			return false;
		}
		// is this rectangle completely above or completely below the other?
		if (this.y + this.h < r.y || this.y > r.y + r.h) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public int compareTo(PointWritable r) {
		if (r instanceof RectangleWritable) {
			return (this.id - ((RectangleWritable) r).id);
		} else
			return -1;
	}
}
