package InvertedIndex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import com.univocity.parsers.annotations.Trim
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.net.URI


object InvertedIndexMain {
  
  def main(args: Array[String]): Unit = {
    
    println("In Main InvertedIndexMain")
    var outFilePath = ""
    outFilePath = "hdfs://quickstart.cloudera:8020/user/cloudera/stopwords/result.txt"
    val fileDir = "hdfs://quickstart.cloudera:8020/user/cloudera/stopwords/shakespeare/"
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val conf = new SparkConf().setMaster("local").setAppName("Inverted - Index")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    
    println("HC is Done !!!")
    
    //Read input file which has stopwords
    val stopWordsTxt = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/stopwords/stopwords.txt")
    
    val stopWords = stopWordsTxt.flatMap { 
      x => x.split("\\r?\\n\\t")
      .map(_.trim)
      }
    
    val stopWordsBroadCast = sc.broadcast(stopWords.collect().toSet)
    
    println(stopWordsBroadCast.value)
    
    val hadoopConf = new org.apache.hadoop.conf.Configuration

    //Deleting output file if exists.
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://quickstart.cloudera:8020"), hadoopConf)
    try { 
      hdfs.delete(new org.apache.hadoop.fs.Path(outFilePath), true) 
      println("File deleted ")
      } catch { 
        case _: Throwable => {} 
        }

      

    val fs = FileSystem.get(new URI(fileDir), hadoopConf)
    
/*    val status = fs.listStatus(new Path(fileDir))
    status.foreach{
      x => println(x.getPath)
      
      }*/
    
    println(stopWordsBroadCast.value)
    sc.wholeTextFiles(fileDir).flatMap {
      case(path,text) =>
        text.replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ")
            .split("""\W+""")
            .filter(stopWordsBroadCast.value.contains(_)).map { 
          word => (path,word)
        }
    }.map{
      case(word,path) => ((word, path), 1)
    }.reduceByKey{
      case(k1,v1) => k1+v1
    }.map{
      case ((word,path),number) => (word,(path,number))
    }.groupBy{
      case(word,(path,number)) => word
    }.map {
      // Output sequence of (fileName, count) into a comma seperated string
      case (w, seq) =>
        val seq2 = seq map {
          case (_, (p, n)) => (p, n)
        }
        (w, seq2.mkString(", "))
    }.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/stopwords/out3")
    
    println("Key is : " )
    
    /*flatMap{
      case(path,text) =>
        
    }*/
    
    
  }
  
}