import org.apache.spark.sql.SparkSession

val spark = SparkSession
    .builder()
    .getOrCreate()

import spark.implicits._

val df = spark.read.json("BigData/Project3/data/transacions.csv")
//val df.show()