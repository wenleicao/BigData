import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd
import java.lang.Math


//////////////////////////////////////////////////////////////////////////////////////////
//define helper functions
object Functions{

//function to map point to cell
    def findCell(p:Array[Int]): Int = {
        Math.floor((p.apply(1)-10000)*(-1)/20).toInt * 500 + Math.floor(p.apply(0)/20+1).toInt
    }

//find neighbors of corner cell
    def findCorner(id:Int, count:Float): Array[(Int,String)] = {
    id match {
	    case 1 => Array((id,count+"|c"),(id+1,count+"|n|"+id),  (id+500,count+"|n|"+id), (id+501,count+"|n|"+id))
	    case 500 => Array((id,count+"|c"), (id-1,count+"|n|"+id), (id+500,count+"|n|"+id),(id+499,count+"|n|"+id))
	    case 249501 => Array((id,count+"|c"),(id+1,count+"|n|"+id), (id-500,count+"|n|"+id),(id-499,count+"|n|"+id))
	    case 250000 => Array((id,count+"|c"), (id-1,count+"|n|"+id), (id-500,count+"|n|"+id), (id-501,count+"|n|"+id))
	}
    }

    def findTop(id:Int, count:Float): Array[(Int,String)] = {
	//println("top")
	Array((id,count+"|c"),(id+1,count+"|n|"+id), (id-1,count+"|n|"+id), (id+500,count+"|n|"+id),  (id+501,count+"|n|"+id), (id+499,count+"|n|"+id))
    }

//find neighbors of cell in bottom row
    def findBottom(id:Int, count:Float): Array[(Int,String)] = {
	//println("bottom")
	Array((id,count+"|c"),(id+1,count+"|n|"+id), (id-1,count+"|n|"+id), (id-500,count+"|n|"+id), (id-501,count+"|n|"+id), (id-499,count+"|n|"+id))
    }

//find neighbors of cell in leftmost col
    def findLeft(id:Int, count:Float): Array[(Int,String)] = {
	//println("left")
	Array((id,count+"|c"), (id+1,count+"|n|"+id), (id+500,count+"|n|"+id), (id-500,count+"|n|"+id), (id+501,count+"|n|"+id), (id-499,count+"|n|"+id))
    }

//find neighbors of cell in rightmost col
    def findRight(id:Int, count:Float): Array[(Int,String)] = {
	//println("right")
	Array((id,count+"|c"), (id-1,count+"|n|"+id), (id+500,count+"|n|"+id), (id-500,count+"|n|"+id), (id+499,count+"|n|"+id), (id-501,count+"|n|"+id))
    }

//map cell to neighboring cells 
//for each output neighbor id, tag, and count of points in this cell
//output: list of tuples (id, "count|tag")
def findNeighbors(id:Int, count:Float): Array[(Int,String)] = {
	var row = id/500
	var col = id % 500

	//handle edge cases
	var corners = Array(1, 500, 249501,250000)
	if( corners.contains(id)) findCorner(id, count)
	else if (col == 0) findRight(id,count)
	else if (col == 1) findLeft(id,count)
	else if(row == 0) findTop(id, count)
	else if(row == 499) findBottom(id, count)
	
	//regular cell
	else Array((id,count+"|c"),(id+1,count+"|n|"+id), (id-1,count+"|n|"+id), (id+500,count+"|n|"+id), (id-500,count+"|n|"+id), (id+501,count+"|n|"+id), (id-501,count+"|n|"+id), (id+499,count+"|n|"+id), (id-499,count+"|n|"+id))
    }

//Function to find the density of one cell
//arg tags should be result of findNeighbors function
//returns density floating point value
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

//Function to find the relative density of neighbors
//arg tags should be result of findNeighbors function
//returns String of neighbor ids and density "id_1|d_1, id_2|d_2, ... id_n|d_n"
     def listDensity(tags:Iterable[String]): String = { 

	//first get this cell's density
	var c = ""
	var n = ""
	
	//then get the density of neighbors
        for (x <- tags){
            val cell = x.split('|')

	    if(cell.apply(1) == "n"){ //neighbor
		n += cell.apply(2)
		n += "|"
		n += cell.apply(0)
		n += ","
            }
	    else{
	   	c = cell.apply(0) + ","
	    }
	
	}	
	return c+n.dropRight(1)
    }
}

// end of helper functions
//////////////////////////////////////////////////////////////////////////////////////////


//read point data
val points = sc.textFile("/home/mqp/BigData/Project3/Problem2/points.csv")

//part1

//count the number of points in each cell

//mapper outputs one record for each point in cell
//reducer sums number of points in each cell
//output: (cell_id, count)

val cellpointcountRDD = points.map(point => (Functions.findCell(point.split(",").map(_.toInt)), 1)).reduceByKey(_ + _)

//map each cell to partition with itself and neighbors
//reduce - compute density for each cell
//output: (cell_id, density)

val densityRDD = cellpointcountRDD.flatMap{ case (id, count) => Functions.findNeighbors(id, count) }.groupByKey().map{ case (id, cells) => (id, Functions.compDensity(cells))}


//list first 50 based on the value of relative density index
densityRDD.takeOrdered(50)(Ordering[Float].reverse.on(x=>x._2)).foreach(println)

//part 2

//map the density of each cell to itself and its neighbors 
//then reduce to get list of ids and densities
//output: (id, n1_id|n1_density, n2_id|2_density, ... , n2_id|2_density)

val neighborDensityRDD = densityRDD.flatMap{ case (id, count) => Functions.findNeighbors(id, count) }.groupByKey().map{ case (id, cells) => (id, Functions.listDensity(cells))}

//list first 50 based on the value of relative density index
neighborDensityRDD.takeOrdered(50)(Ordering[Float].reverse.on(x=>x._2.split(",").apply(0).toFloat)).foreach(println)















