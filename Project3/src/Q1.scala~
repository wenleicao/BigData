import org.apache.spark.sql.types._
//http://spark.apache.org/docs/latest/sql-programming-guide.html#interoperating-with-rdds

// Create an RDD
val transRDD = spark.sparkContext.textFile("BigData/Project3/data/transacions.csv")

// The schema is encoded in a string
val schemaString = "transID custID transTotal transNumItems transDesc"

// Generate the schema based on the string of schema
val fields = schemaString.split(" ")
  .map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)

// Convert records of the RDD (people) to Rows
val rowRDD = transRDD
  .map(_.split(","))
  .map(attributes => Row(attributes(0), attributes(1).trim))

// Apply the schema to the RDD
val transDF = spark.createDataFrame(rowRDD, schema)

// Creates a temporary view using the DataFrame
transDF.createOrReplaceTempView("transacations")

//T1: Filter out the transactions from T whose total amount is less than $200
val T1 = spark.sql("SELECT * FROM transactions") 
//WHERE transTotal < 200")

//val T1.show()



//T2: Over T1 group the transactions by the numer of itemsit has, and for each group calculate the sum of total amounts, and the average of the total amounts, and the min and max of the total amounts.
//Report back T2 t the client side 
//T3: Over T1, group the transactions by customer ID, and for each group report the customer ID and the count of transactions
//T4: Filter out the transactions from T whose total amount is less than $600
//T5: Over T4, group the transactions by customer ID and for each group report the customer ID and the count of transactions
//T6: Selest the customer IDs whose T5.count *3 < T3.count
//Report back T6 to the client side
  	
