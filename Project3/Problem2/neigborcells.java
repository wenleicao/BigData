package neigborcells;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class neigborcells {

	public static void main(String[] args) {
		
				
		int id;
		
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\cell-neighbor-mapping.csv"));
				for (id = 1; id < 250001; id++) {
				
					if (id%500 != 0 && id%500 != 1 ){
					
				bw.write(id + "," + (id-500-1) + "\n"
						+ id + "," + (id-500) + "\n"
						+ id + "," + (id-500+1) + "\n"
						+ id + "," + (id-1) + "\n" 
						+ id + "," + (id+1) + "\n"
						+ id + "," + (id+500-1) + "\n"
						+ id + "," + (id+500) + "\n"
						+ id + "," + (id+500+1) + "\n"							
												
						);
				} 
					
					else
					{
						if (id%500 == 0)
						{
							bw.write(id + "," + (id-500-1) + "\n"
									+ id + "," + (id-500) + "\n"
									//+ id + "," + (id-500+1) + "\r\n"
									+ id + "," + (id-1) + "\n" 
									//+ id + "," + (id+1) + "\r\n"
									+ id + "," + (id+500-1) + "\n"
									+ id + "," + (id+500) + "\n"
									//+ id + "," + (id+500+1) + "\r\n"							
															
									);
						}
						if (id%500 == 1)
						{
							bw.write(//id + "," + (id-500-1) + "\r\n"
									 id + "," + (id-500) + "\n"
									+ id + "," + (id-500+1) + "\n"
									//+ id + "," + (id-1) + "\r\n" 
									+ id + "," + (id+1) + "\n"
									//+ id + "," + (id+500-1) + "\r\n"
									+ id + "," + (id+500) + "\n"
									+ id + "," + (id+500+1) + "\n"							
															
									);
						}
												
						
					}
				
				
			}
				bw.flush();
				bw.close();
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
