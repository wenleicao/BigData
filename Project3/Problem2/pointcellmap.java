package pointcellmap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.FileWriter;


public class pointcellmap {

	public static void main(String[] args) {
		BufferedReader br = null;
		FileReader fr = null;

		try {

			fr = new FileReader("D:\\points.csv");
			br = new BufferedReader(fr);
			BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\pointsMap.csv"));
			String sCurrentLine;
			StringBuilder  sOutput =  new StringBuilder () ;


			//map point to cell by the logic in formula
			while ((sCurrentLine = br.readLine()) != null) {
				//System.out.println(sCurrentLine);
				String Points [] = sCurrentLine.split(",");
				int x = Integer.parseInt(Points [0]);
				int y = Integer.parseInt(Points [1]);
				int cellnumber = (int)Math.floor((y-10000)*(-1)/20)*500 + (int)(Math.floor(x/20+1));
				//System.out.println(x+","+y+","+cellnumber);
				sOutput = sOutput.append(x+","+y+","+cellnumber + "\r\n");
				
			}
			bw.write(sOutput.toString());
			bw.flush();
			bw.close();
			
		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}

		}



	}

}
