//first load transactions.csv to T

 val T = sc.textFile("file:///home/mqp/Documents/transactions.csv")

//T contain 5 columns, TransID, CustID, TransTotal, TransNumItems, TransDesc

// need to add column name to the RDD to use spark sql

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val schemaString = "TransID CustID TransTotal TransNumItems TransDesc"

// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType};


// Generate the schema based on the string of schema
val schema =
  StructType(
    schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))


//spit row
val rowRDD = T.map(_.split(",")).map(p => Row(p(0).toInt, p(1).toInt, p(2).toFloat, p(3).toInt, p(4).trim))


// create data frame
val T_DataFrame = sqlContext.createDataFrame(rowRDD, schema)


// Register the DataFrames as a table.
T_DataFrame.registerTempTable("transaction")

//Q1. filter out transtotal amount < 200
val T1 = sqlContext.sql("SELECT * FROM transaction where TransTotal >= 200")

T1.select("transtotal").show()
T1.registerTempTable("transaction1")


//Q2 over T1,  group transaction by transnumitems, sum transtotal, average, max and min
val T2= sqlContext.sql("select TransNumItems, sum(TransTotal), sum(TransTotal)/count(TransTotal), max(TransTotal), min(TransTotal) from transaction1 group by TransNumItems") 

T2.show()

//Q3. Report back to client side   (not sure what this means)
val T2b= sqlContext.sql("select CustID, sum(TransTotal), sum(TransTotal)/count(TransTotal), max(TransTotal), min(TransTotal) from transaction1 group by CustID")
T2b.show() 

//Q4. over T1, group by customerid and transcation count
val T3= sqlContext.sql("select CustID, Count(*) from transaction1 group by CustID")
T3.show() 


//Q5. over T, filter out total amount <600
val T4 = sqlContext.sql("SELECT * FROM transaction where TransTotal >= 600")
T4.show()


 


