import org.apache.spark.sql.types._
//val T = sc.textFile("file:///home/mqp/Documents/transactions.csv")

// Create an RDD
val T = spark.sparkContext.textFile("file:///home/mqp/Documents/transactions.csv")

// The schema is encoded in a string
val schemaString = "TransID CustID TransTotal TransNumItems TransDesc"

// Generate the schema based on the string of schema
val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)

import org.apache.spark.sql.Row;
// Convert records of the RDD  to Rows
val rowRDD = T.map(_.split(",")).map(attributes => Row(attributes(0),attributes(1),attributes(2),attributes(3), attributes(4).trim))

// Apply the schema to the RDD
val T_DataFrame = spark.createDataFrame(rowRDD, schema)

// Creates a temporary view using the DataFrame
T_DataFrame.createOrReplaceTempView("transaction")

//Q1. filter out transtotal amount < 200
val T1 = sqlContext.sql("SELECT * FROM transaction where TransTotal >= 200")

T1.select("transtotal").show()
T1.createOrReplaceTempView("transaction1")

//Q2 over T1,  group transaction by transnumitems, sum transtotal, average, max and min
val T2= sqlContext.sql("select TransNumItems, sum(TransTotal), sum(TransTotal)/count(TransTotal), max(TransTotal), min(TransTotal) from transaction1 group by TransNumItems") 


//Q3 Report back to client side
T2.show()


//write T_DataFrame and T1 into parquet file in case they are replaced in memory
import spark.implicits._
T_DataFrame.write.parquet("Transaction.parquet")
T1.write.parquet("T1.parquet")

//Q4. over T1, group by customerid and transcation count
val T3= sqlContext.sql("select CustID, Count(*) as transactioncount from transaction1 group by CustID")
T3.show() 


//Q5. over T, filter out total amount <600
val T4 = sqlContext.sql("SELECT * FROM transaction where TransTotal >= 600")
T4.show()

//Q6. over T4, group by customerid and transcation count
T4.createOrReplaceTempView("transaction4")
val T5= sqlContext.sql("select CustID, Count(*) as transactioncount from transaction4 group by CustID")
T5.show()

//Q7. select customerID whose T5.count*3 < T3.count
T3.createOrReplaceTempView("transaction3b")
T5.createOrReplaceTempView("transaction5b")
val T6 = sqlContext.sql("select T3.CustID from transaction3b T3 inner join transaction5b T5 where T3.CustID = T5.CustID and T5.transactioncount*3 < T3.transactioncount ")


//Q8 Report back to client side
T6.show()




















