package others

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object ReadDirFiles  extends java.io.Serializable {
  
  def main(args: Array[String]): Unit = {
    
    println("In Main")
    
    val conf = new SparkConf().setMaster("local[4]").setAppName("Test")
    val sc = new SparkContext(conf)
    
    println("SC is built")
    
    val inpDir = sc.wholeTextFiles("/home/cloudera/DataSets/a*")
    
    println("Keys : " ) 
    inpDir.keys.collect().foreach {println }
    println("Values : " )
     inpDir.foreach(println)
    
     inpDir.values.foreach(x => sc.parallelize(x).saveAsTextFile("/home/cloudera/DataSets/savedFileValues3"))
    // inpDir.keys.saveAsTextFile("/home/cloudera/DataSets/savedFileKeys")
     
  /*  
    inpDir.map{ case (fileName,content) => 
          println("File is : " + fileName)
          val fil = sc.textFile(fileName)
          println("Length is : " +fil.collect.length)}
    inpDir.mapValues { x => println("Lenght is : " +x.length()) } */
    
  }
  

}