package sjoin;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;

/**
 * Program that create two datasets (two comma separated files), Customers and
 * Transactions.
 * 
 * @author caitlin
 *
 */
public class DataGenerator {

	static Random random = new Random();

	public static void main(String[] args) {

		/**
		 * Generator for spatial join data.
		 * Generates two sets of data: P random points in space and R random rectangles in space.
		 * Space assumed to be 2d from 1 to 10000 along each axis
		 */

		BufferedWriter bw = null;
		try {

			int x;
			int y;
			int id;

			bw = new BufferedWriter(new FileWriter("points.csv"));
			for (id = 1; id < 12000000; id++) {
				
				x = random.nextInt(10000) + 1;
				y = random.nextInt(10000) + 1;
				bw.write(x + "," + y + "\n");
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {

			int x;
			int y;
			int h;
			int w;
			int id;

			bw = new BufferedWriter(new FileWriter("rectangles.csv"));
			for (id = 1; id < 2500000; id++) {

				x = random.nextInt(10000) + 1;
				y = random.nextInt(10000) + 1;
				h = random.nextInt(20) + 1;
				w = random.nextInt(5) + 1;
				
				bw.write(x +","+ y +","+ h+","+ w +"\n");
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Done");
	}

}
