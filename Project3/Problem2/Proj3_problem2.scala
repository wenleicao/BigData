//create dataframe for point cell map

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val PC = spark.sparkContext.textFile("file:///home/mqp/Documents/pointsMap.csv")
val schemaString = "x y cell"
val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)
val rowRDD = PC.map(_.split(",")).map(attributes => Row(attributes(0),attributes(1),attributes(2).trim))
val PC_DF = spark.createDataFrame(rowRDD, schema)
PC_DF.createOrReplaceTempView("PointsCellMap")
val PC1 = sqlContext.sql("SELECT * FROM PointsCellMap")
PC1.show()




//create dataframe for cell-neighbor cell map
 
val CNC = spark.sparkContext.textFile("file:///home/mqp/Documents/cell-neighbor-mapping.csv")
val schemaString = "cell neighbor_cell"
val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)
val rowRDD = CNC.map(_.split(",")).map(attributes => Row(attributes(0),attributes(1).trim))
val CNC_DF = spark.createDataFrame(rowRDD, schema)
CNC_DF.createOrReplaceTempView("NeighborCellMap")
val CNC1 = sqlContext.sql("SELECT * FROM NeighborCellMap where neighbor_cell >0 and neighbor_cell <=250000" )
CNC1.show()
//create view for real neighborCellmap
CNC1.createOrReplaceTempView("RealNeighborCellMap")


//need to first get how many point in each cell
val PointCountInCell = sqlContext.sql("SELECT cell, count(*) as PointCount from PointsCellMap group by cell")
PointCountInCell.show() 
PointCountInCell.createOrReplaceTempView("CellPointCount")


//need to calculate each cell's neighbor cell totoal count and cell number for calcuate average
 
val NeigborCellPointCount = sqlContext.sql("select ncm.cell, sum(cpc.Pointcount) as NeighborPointCount, count(neighbor_cell) as NeighborCellCount from CellPointCount cpc inner join RealNeighborCellMap ncm where ncm.neighbor_cell = cpc.cell group by ncm.cell")
NeigborCellPointCount.show()
NeigborCellPointCount.createOrReplaceTempView("NeighborCellPointCount")


//calculate the Relative Density index based on formula, list top 50
val Top50RDICell = sqlContext.sql("select ncp.cell,  cpc.PointCount/(ncp.NeighborPointCount/ncp.NeighborCellCount) as Relative_Density_index from CellPointCount cpc inner join NeighborCellPointCount ncp where ncp.cell = cpc.cell order by cpc.PointCount/(ncp.NeighborPointCount/ncp.NeighborCellCount) desc limit 50")
Top50RDICell.collect().foreach(println)


//report neighbor cell id and their RDI
Top50RDICell.createOrReplaceTempView("Top50RDICell")
val neighborCellinfo =sqlContext.sql("select t50.cell, ncm.neighbor_cell, cpc.PointCount/(ncp.NeighborPointCount/ncp.NeighborCellCount) as neighbor_Relative_Density_index  from Top50RDICell t50 inner join RealNeighborCellMap ncm inner join CellPointCount cpc inner join NeighborCellPointCount ncp where t50.cell = ncm.cell and ncm.neighbor_cell =cpc.cell and ncm.neighbor_cell =ncp.cell order by t50.cell, ncm.neighbor_cell ")

//show only 20
neighborCellinfo.show()

//show all
neighborCellinfo.collect().foreach(println)











