package test

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object Test16 {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[1]").setAppName("graph")
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List((11, 22), (22, 33), (33, 44), (44, 55), (55, 66)))
    val edgeRDD = input.map(ele => new Edge(ele._1, ele._2, None))
    val graph = Graph.fromEdges(edgeRDD, None)


    graph.vertices.foreach(println(_))
  }

}
