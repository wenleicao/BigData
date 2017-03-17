
//need  cell  pointcount

val PC = sc.textFile("file:///home/mqp/Documents/pointsMap.csv")

// get only cell number and map each record with 1, to do point count

val PC_Count = PC.map(rec => (rec.split(",")(2).toInt, 1))

//do point count
val cellpointcount = PC_Count.countByKey()

//collection to RDD
val cellpointcountRDD = sc.parallelize(cellpointcount.toSeq,1)



//need cell neighbor cell count

val NC = sc.textFile("file:///home/mqp/Documents/cell-neighbor-mapping.csv")
//filter in only between cell 1 and cell 250000
val RNC = NC.map(rec=>(rec.split(",")(0).toInt, rec.split(",")(1).toInt)).filter(x=>x._2> 0).filter(x=>x._2< 250001)


//cell neighbor cell count
val cellneighborcellcount = RNC.countByKey()

//collection to RDD
val cellneighborcellcountRDD = sc.parallelize(cellneighborcellcount.toSeq,1)



//need cell neighbor cell point count,  use NC, but need to use neighbor cell for join, therefore reverse the squence in tuple

val RNC_Reverse = RNC.map(x=>(x._2, x._1))

//join cellpointcountRDD with RNC_Reverse based on they key   (neighborcell (cell, cell count))

val cellneighborcelljoin = cellpointcountRDD.join(RNC_Reverse)

//need to adjust the tuple, to let cell number as key to calculate  cell neighbor cell point count

val neighborcellpointcount = cellneighborcelljoin.map(rec=>(rec._2._2, rec._2._1))

//sum the value based on cell number

val neighborcellpointcountRDD = neighborcellpointcount.reduceByKey((acc, value)=>acc+value)




//need to join these three dataset togehter to calculate index

val pointjoin2 = cellpointcountRDD.join(cellneighborcellcountRDD) 

val pointjoin3 = pointjoin2.join(neighborcellpointcountRDD)


//after two join, it form such structure    spark.rdd.RDD[(Int, ((Long, Long), Long))]
// int is cell number,  first long   cellpointcount   rec._2._1._1;  second long:neighbor cell count: rec._2._1._2; third long  neighbor cell point count: rec._2._2
//per formula  x / (y/cell count)
//use toFloat to show decimal

val RelativeDensityIndex = pointjoin3.map(rec=>(rec._1, (rec._2._1._1.toFloat/(rec._2._2/rec._2._1._2))) )   

//show some vlaue of RDI
RelativeDensityIndex.take(5).foreach(println)


//list first 50 based on the value of relative density index

val RDI_top50 = RelativeDensityIndex.takeOrdered(50)(Ordering[Float].reverse.on(x=>x._2))


//show top 50 cell and RDI
RDI_top50.take(50).foreach(println) 


//need to get top 50 cell's neighbor cell number  and its RDI
//first join the RDI_top50 to RNC to get their neibor cell
// remap it into a RNC_reverse1  (neighbor cell, cell)
//RelativeDensityIndex contain all cell number and corresponding RDI,  we need to join  RNC_reverse1,  neighbor cell number, so we get (neighbor cell,  (RDI, cell)), once we get result, we remap the position and then got top 50 of them

//first convert RDI_top50 to RDD

val RDI_top50_RDD= sc.parallelize(RDI_top50.toSeq,1)

val top50neigborcell = RDI_top50_RDD.join(RNC)

// joint result (Int, (Float, Int)) = (111635,(1.7045455,111134)), need to pick neigbor cell, rec._2._2 and current cell,  rec._1
val RNC_reverse1 = top50neigborcell.map(rec=>(rec._2._2, rec._1))

// RelativeDensityIndex  join with RNC_reverse1
val RDI_top50_neighborRDI = RelativeDensityIndex.join(RNC_reverse1)

//result structure res12: (Int, (Float, Int)) = (177102,(1.1702127,177103)), neibor cell, RDI, and cell, I need to remap and show all

val top50cell_neighborcell_RDI= RDI_top50_neighborRDI.map(rec=>(rec._2._2,rec._1, rec._2._1 ))

//show all sort by cell numer 

top50cell_neighborcell_RDI.sortBy(_._1).collect().foreach(println)
































