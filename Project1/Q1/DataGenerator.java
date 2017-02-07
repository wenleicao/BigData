package Q1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;

/**
 * Program that create two datasets (two comma separated files), Customers and Transactions.
 * @author caitlin
 *
 */
public class DataGenerator {
	
	static Random random = new Random();
	
	
	public static void main(String[] args) {
		
//		The	Customers dataset:
//		ID:	unique	sequential	number	(integer)	from	1	to	50,000	(that	is	the	file	will	have	50,000	line)
//		Name:	random	sequence	of	characters	of	length	between	10	and	20	(do	not	include	commas)
//		Age:	random	number	(integer)	between	10	to	70
//		CountryCode:	random	number	(integer)	between	1	and	10
//		Salary:	random	number	(float)	between	100	and	10000

		try (BufferedWriter bw = new BufferedWriter(new FileWriter("customers.csv"))) {
			
			int id;
			int age;
			int countryCode;
			String name;
			float salary;
			
			for(id=1 ; id<50001 ; id++){
				age = random.nextInt(61) + 10;
				countryCode = random.nextInt(10) + 1;
				name = RandomStringUtils.randomAlphabetic(random.nextInt(11)+10);
				salary = (random.nextFloat()*9900)+100;
				bw.write(id+","+name+","+age+","+countryCode+","+salary+"\n");
			}

		} catch (IOException e) {

			e.printStackTrace();

		}
		
//		The	Transactions dataset:
//		TransID:	unique	sequential	number	(integer)	from	1	to	5,000,000	(the	file	has 5M	transactions)
//		CustID:	References	one	of	the customer	IDs,	i.e.,	from	1	to	50,000 (on	Avg.	a customer	has	100 trans.)
//		TransTotal:	random	number	(float)	between	10	and	1000
//		TransNumItems:	random	number	(integer)	between	1	and	10
//		TransDesc:	random	text	of	characters	of	length	between	20	and	50	(do	not	include	commas)
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter("transactions.csv"))) {

			int id;
			int custID;
			float transTotal;
			int transNumItems;
			String transDesc;
			
			
			for(id=1 ; id<5000001 ; id++){
				custID = random.nextInt(50000) + 1;
				transTotal = (random.nextFloat()*990)+10;
				transNumItems = random.nextInt(10) + 1;
				transDesc = RandomStringUtils.randomAlphabetic(random.nextInt(31)+20);
				
				bw.write(id+","+custID+","+transTotal+","+transNumItems+","+transDesc+"\n");
			}

		} catch (IOException e) {

			e.printStackTrace();

		}
		System.out.println("Done");
	}

}

