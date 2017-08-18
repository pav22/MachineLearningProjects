package airlines.analysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row


object AirlinesAnalyissMain {
  
  def main(args: Array[String]): Unit = {
    
    println("In Main Method")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val conf = new SparkConf().setMaster("local").setAppName("Airlines")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    
    println("HiveContext is build")
    
      val schema = StructType(Array(StructField("citypair",StringType),
                      StructField("fromlocation",StringType),
                      StructField("tolocation",StringType),
                      StructField("producttype",StringType),
                      StructField("adults",StringType),
                      StructField("seniors",StringType),
                      StructField("children",StringType),
                      StructField("youth",StringType),
                      StructField("infant",StringType),
                      StructField("dateoftravel",StringType),
                      StructField("meoftravel",StringType),
                      StructField("dateofreturn",StringType),
                      StructField("timeofreturn",StringType),
                      StructField("priceofbooking",StringType),
                      StructField("v1",StringType),
                      StructField("v2",StringType),
                      StructField("airlinescode",StringType),
                      StructField("airlinename",StringType)))
                      /*,
                      StructField("hotelname",StringType)))*/
          
    
    
    val fileWithOutSplit = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/TravelData.txt")
    val withSplit = fileWithOutSplit.map{
      x=> x.split("\t")
    }
    
    withSplit.foreach { x => println("Lenght  is :" + x.length) }
    
    val withRow = withSplit.filter { x => x.length == 17 }.map {
        x=> Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16))
      }
    val rdd = hc.createDataFrame(withRow,schema)
    
    rdd.printSchema()
    
 //   rdd.foreach{x=> println(x)}
    

    val redBy = rdd.map(x=> (x(2),1)).reduceByKey(_+_)
    redBy.foreach(println)
    
    println("After Sort")
    
    val sorted = redBy.sortBy(x=> x._2,false)
    sorted.take(5).foreach(println)
    
    println("Case 2 Analysis !!!!!!!!!!!!!!!!!!!!!!!!!!")
    
    
    
    println("End of Object")
    
  }

  def addDefaultVallue(value:String) : String = {
      var retVal = ""
      if(value.equalsIgnoreCase("")){
        retVal = "null"
      }
      retVal
    
  }
  
/*  
  (citypair:Option[String], fromlocation:Option[String],tolocation:Option[String],producttype:Option[Integer],adults:Option[Integer],
                    seniors:Option[Integer],children:Option[Integer],youth:Option[Integer],infant:Option[Integer],dateoftravel:Option[String],timeoftravel:Option[String],
                    dateofreturn:Option[String],timeofreturn:Option[String],priceofbooking:Option[Float],v1:Option[Float],v2:Option[Float],airlinescode:Option[String],airlinename:Option[String],
                    hotelname:Option[String])
*/  
}