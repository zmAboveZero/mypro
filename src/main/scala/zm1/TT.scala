package zm1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TT {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: SparkConf = new SparkConf().setAppName("t").setMaster("local[*]")
    val sc = new SparkContext(conf)
  }

}
