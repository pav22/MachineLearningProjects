package moviesData

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

import org.apache.spark.mllib.recommendation.{ALS,  MatrixFactorizationModel}

object MLLibMain {
  
  def main(args: Array[String]): Unit = {
    
    println("Main Object Started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local").setAppName("MLLib").set("spark.executor.memory","2g")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    
    println("SC Build Successfully ")
    
    val movies = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/movies.csv")
  //  println(movies.first())
    val head = movies.first()
    
    val moviesData = movies.filter { x => !x.equalsIgnoreCase(head) }
    val moviesDataSplit = moviesData.map { 
      x =>
      val fields = x.split(",")
      // format: (movieId, movieName)
      (fields(0),fields(1))
        }.collect.toMap
    
    val ratings = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ratings.csv")
    //println(ratings.take(3).foreach { println })
    
    val ratHead = ratings.first()
    val ratingsData = ratings.filter { x => !x.equals(ratHead) }
        
    val ratingsSplit = ratingsData.map { 
      line =>
        val fields = line.split(",")
        // format: (timestamp % 10, Rating(userId, movieId, rating))
        (fields(3).toLong, Rating(fields(0).toInt,fields(1).toInt,fields(2).toDouble))
        }
    
     
    println("Datasets are loaded")
    
    val ratingsSplitData =  ratingsSplit.randomSplit(Array(0.7,0.3), seed=11L)
    val ratingSplitOne = ratingsSplitData(0)
    
    val ratingTrainingValidation = ratingSplitOne.randomSplit(Array(0.7,0.3), seed=11L)
    
    val ratingTraining = ratingTrainingValidation(0)
    val ratingValidation = ratingTrainingValidation(1)
    val ratingTest = ratingsSplitData(1)
        
    val numValidation = ratingValidation.count
    val ranks = List(8,12)
    val lambdas = List(1.0,10.0)
    val numIters = List(10,20)
    var bestModel : Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    
    val ratingValidation2 = ratingValidation.map(f=> (f._2))
    val ratingTraining2 = ratingTraining.map(f => (f._2))
    
//    for(rank <- ranks; lambda <- lambdas; numIter <- numIters){
//      val model =  ALS.train(ratingTraining2, rank, numIter, lambda)
//      val validationRmse = computeRmse(model, ratingValidation2, numValidation)
//    }

        
    
  }
  
    /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Long)
    : Double = {

    val predictions = model.predict(data.map(x => (x.userId, x.movieId)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.userId, x.movieId), x.rating)))
       .values
    math.sqrt(predictionsAndRatings.map(x =>(x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / implicitPrefs)
  }
  

  
  case class Rating(userId:Int,movieId:Int,rating:Double)
}