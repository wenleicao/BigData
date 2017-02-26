package kmeans;

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
		 * Generator for kmeans data.
		 * Generates set of random 2d points in space.
		 * Space assumed to be 2d from 1 to 10000 along each axis
		 */

		BufferedWriter bw = null;
		try {

			int x;
			int y;
			int id;

			bw = new BufferedWriter(new FileWriter("kmeanspoints.csv"));
			for (id = 1; id < 12000000; id++) {
				
				x = random.nextInt(10000);
				y = random.nextInt(10000);
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

		System.out.println("Done");
	}

}
