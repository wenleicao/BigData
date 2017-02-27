import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class RectangleWritable implements WritableComparable<RectangleWritable> {


		int x;
		int y;
		int h;
		int w;
		

		public RectangleWritable() {
		}

		public RectangleWritable(int x, int y, int h, int w) {
			this.x = x;
			this.y = y;
			this.h = h;
			this.w = w;
		}

	

		public RectangleWritable(String line) {
			String[] split = line.split(",");
			this.x = Integer.parseInt(split[0]);
			this.y = Integer.parseInt(split[1]);
			this.h = Integer.parseInt(split[2]);
			this.w = Integer.parseInt(split[3]);
			
		}
		
	
		@Override
		public void readFields(DataInput in) throws IOException {
			x = in.readInt();
			y = in.readInt();
			h = in.readInt();
			w = in.readInt();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(x);
			out.writeInt(y);
			out.writeInt(h);
			out.writeInt(w);
			
		}

		@Override
		public String toString() {
			return x + "," + y + "," + h + "," + w  ;
		}

		public boolean inRectangle(PointWritable P) {

			return P.x >= this.x && P.x < (this.x +this.w)    ;
		}

	
			

		public void clear() {
			this.x = 0;
			this.y = 0;
			this.h = 0;
			this.w =0;
		}

			

		@Override
		public int compareTo(RectangleWritable r) {
			// TODO Auto-generated method stub
			return (this.x-r.x)+(this.y-r.y) + (this.h -r.h) + (this.w -r.w);
		}
	}

	
	
	

