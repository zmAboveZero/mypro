import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object UUU {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setAppName("fs").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    println("fss")
    print("88888")
  }

}
