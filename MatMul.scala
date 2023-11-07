import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Multiply")
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    val sc = new SparkContext(conf)
    
    // matrix m as input
    val matrixM = sc.textFile(args(0)).map( line => 
    { 
      val a = line.split(",")                                         
      ((a(0).toInt,a(1).toInt),a(2).toDouble) 
      } )
                                                
    // matrix n as input
    val matrixN = sc.textFile(args(1)).map( line => 
    { 
      val a = line.split(",")                                     
      ((a(0).toInt,a(1).toInt),a(2).toDouble) 
      } )
    
    // calculation
    // Perform matrix multiplication by joining and reducing the two RDDs
    val product = matrixM.map{case ((i, j), v) => (j, (i,v))}
      .join(matrixN.map{ case ((j,k),v) => (j,(k,v))}) 
      .map{case (j, (m, n)) => ((m._1, n._1), (m._2 * n._2))}
      .reduceByKey(_ + _)
    // to know how many partitions does it makes  
    val numpartitions = product.getNumPartitions
    //sort by key sort s the value based on key formed and true indicates Ascending order if you give .sortByKey(true,0) it gives you onle one txt as o/p instaed of partitions number txt's 
    val result = product.sortByKey(true)
    // Save the result to a text file
    result.foreach(println)
    println(s"Number of partitions : $numpartitions")
    result.map{case ((i,j), v) => s"$i,$j,$v"}.saveAsTextFile(args(2))
    
    
    
    

    sc.stop()
    

  }
}
