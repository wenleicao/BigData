package Query2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataGenerator {

	public static void main(String[] args) {
		
		int x;
		int y;
		int id;
		Random random = new Random();
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\points.csv"));
				for (id = 1; id < 12000000; id++) {
				x = random.nextInt(10000) + 1;
				y = random.nextInt(10000) + 1;
				bw.write(x + "," + y + "\n");
				
			}
				bw.flush();
				bw.close();
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         
	}

}
