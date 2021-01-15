package test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Test17 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
    val sc = new SparkContext(conf)
    val unit: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    val value: RDD[(Int, Iterable[Int])] = unit.groupBy(ele=>ele)

    val ss = SparkSession.builder().master("local[2]").appName("test").getOrCreate()
    import ss.implicits._
    val df = ss.sparkContext.parallelize(List(1, 2, 2, 3, 5)).toDF("hello").createTempView("tmp")


//    println(ss.sql(
//      """
//        |select count(1) from tmp group by hello
//      """.stripMargin).rdd.partitioner)

    ss.sql(
      """
        |select 88<>99
      """.stripMargin).show()

  }

}
