package uberData.kmeans

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.KMeans

import org.apache.spark.ml.feature
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.storage.StorageLevel




//import org.apache.spark.mllib.clustering.KMeans


object KMeansMain {

  def main(args: Array[String]): Unit = {

    println("Main KMeansMain")
    println(new Date)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[12]").setAppName("KMeans")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    println("HiveContext Build Successfully !!!")
    
    val schema = StructType(Array(StructField("date",TimestampType,true),
                                  StructField("lat",DoubleType,true),
                                  StructField("long",DoubleType,true),
                                  StructField("base",StringType,true)))

          
    val uberLoadFile =  sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/uber_raw_data_sep14.csv")
    val uberHead = uberLoadFile.first()                 
                      
    val uberLoadSplitted = uberLoadFile.filter { x => !x.equalsIgnoreCase(uberHead) }.map { x => x.split(",") }
    
      
    val uberLoadmap = uberLoadSplitted.map{ x => 
        Row(convertToTimeStamp(x(0).replaceAll("\"", "")),x(1).replaceAll("\"", "").toDouble,x(2).replaceAll("\"", "").toDouble,x(3).replaceAll("\"", ""))}

    
    val uberWithSchema = hc.createDataFrame(uberLoadmap, schema)
      println("Head of Data : " + uberWithSchema.head())

      val df = uberWithSchema    
      df.cache()
    println(df.show())
    println(df.printSchema())
    
    
    val featureCols = Array("lat","long")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    
    val df2 = assembler.transform(df)
    df2.cache()
    println("Showing df2 ")
    df2.show()
    
    
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7,0.3),5043)
    trainingData.cache()
    println("Summary of training Data")
    val trainingDescribe = trainingData.describe()
    
    import org.apache.spark.sql.functions._
    
    trainingDescribe.select("mean")
    trainingDescribe.columns
    
    testData.cache()
    println("Summary of training Data")
    testData.describe().show
    
    
    val kmeans = new KMeans
    val km = kmeans.setK(20).setFeaturesCol("features").setPredictionCol("predection")
    val model = km.fit(df2)

    
    
    println("Result is :")
    model.clusterCenters.foreach(println)

  
   
    
    println("Using testData now ")
    val categories = model.transform(testData)
    categories.show()
    
     println("Below is the describe ")
     
     categories.describe().show
    //categories.save("hdfs://quickstart.cloudera:8020/user/cloudera/uberModelData", "text")
   // println("going to save")
    //categories.write.format("csv").save("hdfs://quickstart.cloudera:8020/user/cloudera/uberModelData4")
    println("Saved to file")
    
    
    
    println("Grouped by Predection :")
    categories.groupBy("predection").count.show
      println(new Date)
  }
  
  
  def convertToTimeStamp(x:String) : Timestamp = {
   // println("In convert method")
    val format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    if(x.toString() == "")
      return null
    else
    {
      //println("Date is : " + x)
      //Date is : "9/1/2014 0:01:00"

      val d = format.parse(x)
    //  println("Formatted :; " + d)
      val formattedDate = new Timestamp(d.getTime())
      return formattedDate
    }
    
    
  }
  
  
  
  
}