import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd
import java.lang.Math


//////////////////////////////////////////////////////////////////////////////////////////
//define helper functions
object Functions{

    def findCell(p:Array[Int]): Int = {
        Math.floor((p.apply(1)-10000)*(-1)/20).toInt * 500 + Math.floor(p.apply(0)/20+1).toInt
    }

    def findCorner(id:Int, count:Float): Array[(Int,String)] = {
    id match {
	    case 1 => Array((id,count+"|c"),(id+1,count+"|n"),  (id+500,count+"|n"), (id+501,count+"|n"))
	    case 500 => Array((id,count+"|c"), (id-1,count+"|n"), (id+500,count+"|n"),(id+499,count+"|n"))
	    case 249501 => Array((id,count+"|c"),(id+1,count+"|n"), (id-500,count+"|n"),(id-499,count+"|n"))
	    case 250000 => Array((id,count+"|c"), (id-1,count+"|n"), (id-500,count+"|n"), (id-501,count+"|n"))
	}
    }

    def findTop(id:Int, count:Float): Array[(Int,String)] = {
	//println("top")
	Array((id,count+"|c"),(id+1,count+"|n"), (id-1,count+"|n"), (id+500,count+"|n"),  (id+501,count+"|n"), (id+499,count+"|n"))
    }

    def findBottom(id:Int, count:Float): Array[(Int,String)] = {
	//println("bottom")
	Array((id,count+"|c"),(id+1,count+"|n"), (id-1,count+"|n"), (id-500,count+"|n"), (id-501,count+"|n"), (id-499,count+"|n"))
    }

    def findLeft(id:Int, count:Float): Array[(Int,String)] = {
	//println("left")
	Array((id,count+"|c"), (id+1,count+"|n"), (id+500,count+"|n"), (id-500,count+"|n"), (id+501,count+"|n"), (id-499,count+"|n"))
    }

    def findRight(id:Int, count:Float): Array[(Int,String)] = {
	//println("right")
	Array((id,count+"|c"), (id-1,count+"|n"), (id+500,count+"|n"), (id-500,count+"|n"), (id+499,count+"|n"), (id-501,count+"|n"))
    }

def findNeighbors(id:Int, count:Float): Array[(Int,String)] = {
	var row = id/500
	var col = id % 500

	//handle edge cases
	var corners = Array(1, 500, 249501,250000)
	if( corners.contains(id)) findCorner(id, count)
	else if(row == 0) findTop(id, count)
	else if(row == 499) findBottom(id, count)
	else if (col == 0) findRight(id,count)
        else if (col == 1) findLeft(id,count)
        
	//regular cell
	else Array((id,count+"|c"),(id+1,count+"|n"), (id-1,count+"|n"), (id+500,count+"|n"), (id-500,count+"|n"), (id+501,count+"|n"), (id-501,count+"|n"), (id+499,count+"|n"), (id-499,count+"|n"))
    }

    def compDensity(tags:Iterable[String]): Float = {
	var d = 0f
	var n = 0f
        for (x <- tags){
            val cell = x.split('|')
	    if(cell.apply(1) == "n"){ //neighbor
		d = d + cell.apply(0).toFloat
		
            }
	    else { //core cell
		n= cell.apply(0).toFloat
	    }
	}	
	if(d == 0) return 0
	else return n/d
    }

     //def listDensity(id:Int, count:Float): Array[(Int,String)] = {println(info)}
}


//////////////////////////////////////////////////////////////////////////////////////////

//read point data
val points = sc.textFile("/home/mqp/BigData/Project3/Problem2/points.csv")


//count the number of points in each cell

//mapper outputs one record for each point in cell
//reducer sums number of points in each cell
//output: cell_id, count

val cellpointcountRDD = points.map(point => (Functions.findCell(point.split(",").map(_.toInt)), 1)).reduceByKey(_ + _)

//map each cell to partition with itself and neighbors
//reduce -  compute density for each cell
//output: cell_id, density

val densityRDD = cellpointcountRDD.flatMap{ case (id, count) => Functions.findNeighbors(id, count) }.groupByKey().map{ case (id, cells) => (id, Functions.compDensity(cells))}

densityRDD.take(5).foreach(println)


//list first 50 based on the value of relative density index
val RDI_top50 = densityRDD.takeOrdered(50)(Ordering[Float].reverse.on(x=>x._2))


//show top 50 cell and RDI
RDI_top50.foreach(println) 


//need to get top 50 cell's neighbor cell number  and its RDI
//first join the RDI_top50 to RNC to get their neibor cell
// remap it into a RNC_reverse1  (neighbor cell, cell)
//RelativeDensityIndex contain all cell number and corresponding RDI,  we need to join  RNC_reverse1,  neighbor cell number, so we get (neighbor cell,  (RDI, cell)), once we get result, we remap the position and then got top 50 of them

//first convert RDI_top50 to RDD

val RDI_top50_RDD= sc.parallelize(RDI_top50.toSeq,1)

//val top50neigborcell = RDI_top50_RDD.join(RNC)

// joint result (Int, (Float, Int)) = (111635,(1.7045455,111134)), need to pick neigbor cell, rec._2._2 and current cell,  rec._1
//val RNC_reverse1 = top50neigborcell.map(rec=>(rec._2._2, rec._1))

// RelativeDensityIndex  join with RNC_reverse1
//val RDI_top50_neighborRDI = RelativeDensityIndex.join(RNC_reverse1)

//result structure res12: (Int, (Float, Int)) = (177102,(1.1702127,177103)), neibor cell, RDI, and cell, I need to remap and show all

//val top50cell_neighborcell_RDI= RDI_top50_neighborRDI.map(rec=>(rec._2._2,rec._1, rec._2._1 ))

//show all sort by cell numer 

//top50cell_neighborcell_RDI.sortBy(_._1).collect().foreach(println)















