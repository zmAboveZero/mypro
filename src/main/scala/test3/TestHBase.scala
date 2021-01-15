package test3

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object TestHBase {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val vertices = sc.makeRDD(Seq((1L, ""), (1L, ""), (2L, ""), (3L, ""), (4L, ""), (4L, "")))
    val edges = sc.makeRDD(Seq(Edge(1L, 1L, ""), Edge(1L, 2L, ""), Edge(2L, 3L, ""), Edge(1L, 3L, ""), Edge(3L, 4L, ""))).filter(e => e.srcId != e.dstId).flatMap(e => Array(e, Edge(e.dstId, e.srcId, e.attr))).distinct() //去掉自循环边，有向图变为无向图，去除重复边
    val testGraph = Graph(vertices, edges).cache()
    println("globalClusteringCoefficient")
    println(globalClusteringCoefficient(testGraph))
    println("localClusteringCoefficient")
    localClusteringCoefficient(testGraph).foreach(println)
    println("avgClusteringCoefficient")
    println(localClusteringCoefficient(testGraph).map(_._2).sum() / testGraph.vertices.count())

  }

  def globalClusteringCoefficient[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) = {
    val numerator = g.triangleCount().vertices.map(_._2).reduce(_ + _)
    val denominator = g.inDegrees.map { case (_, d) => d * (d - 1) / 2.0 }.reduce(_ + _)
    if (denominator == 0) 0.0 else numerator / denominator
  }

  def localClusteringCoefficient[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) = {
    val triCountGraph = g.triangleCount()
    val maxTrisGraph = g.inDegrees.mapValues(srcAttr => srcAttr * (srcAttr - 1) / 2.0)
    triCountGraph.vertices.innerJoin(maxTrisGraph) { (vid, a, b) => if (b == 0) 0 else a / b }
  }

}
