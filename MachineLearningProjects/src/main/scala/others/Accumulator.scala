package others

import scala.collection.mutable
import scala.collection.mutable.HashSet

import org.apache.spark.Accumulable
import org.apache.spark.SparkContext

object Accumulator {
  
  def main(args: Array[String]): Unit = {
    
    
        val maxI = 1000
    for (nThreads <- List(1, 10)) { // test single & multi-threaded
      val sc = new SparkContext("local[" + nThreads + "]", "test")
      val k = new mutable.HashSet[Any]()
    /*  val acc = sc.accumulable(0)
      sc.accumulable(0) 
      val d = sc.parallelize(1 to maxI)
      d.foreach {
        x => acc += x
      }
      val v = acc.value.asInstanceOf[mutable.Set[Int]]
      for (i <- 1 to maxI) {
          println(i)
      }*/
     
    }
    
  }
  
}