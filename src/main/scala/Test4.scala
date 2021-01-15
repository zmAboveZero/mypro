import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object Test4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input = Array("1", "2", "3", "4", "5", "6", "7")
    println(new Test1RDD(input, sc).getNumPartitions)
    println(new Test1RDD(input, sc).getNumPartitions)
    new Test1RDD(input, sc).foreach(println(_))
  }

}
