package pr2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PageRankTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("PRTEST").set("spark.testing.memory", "2147480000"))

    val links = sc.parallelize(List(("A", List("D")), ("B", List("A")), ("C", List("A", "B")), ("D", List("A", "C"))))
    var ranks = sc.parallelize(List(("A", 1.0), ("B", 1.0), ("C", 1.0), ("D", 1.0)))
    //    val value: RDD[(String, Double)] = links.join(ranks).flatMap { case (url, (links, rank)) => links.map(dest => (dest, rank / links.size)) }
    //    value.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _).foreach(println(_))
    for (i <- 1 to 200) {
      val c = links.join(ranks).flatMap { case (url, (links, rank)) => links.map(dest => (dest, rank / links.size)) }
      val value: RDD[(String, Double)] = links.join(ranks).flatMap { case (url, (links, rank)) => links.map(dest => (dest, rank / links.size)) }
      ranks = c.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks.foreach(println(_))
  }


}
