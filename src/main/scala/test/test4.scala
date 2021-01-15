package test

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test4 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
    val sc = new SparkContext(conf)
    val input: RDD[(Long, Long)] = sc.parallelize(Array((1L -> 4L), (2L -> 4L), (3L -> 4L), (5L -> 6L), (6L -> 7L), (7L -> 5L)))
    val edges: RDD[Edge[None.type]] = input.map(e => Edge(e._1, e._2, None))
    val graph: Graph[None.type, None.type] = Graph.fromEdges(edges, None)
    val output: Graph[None.type, None.type] = graph.filter(
      graph => {
        val degrees: VertexRDD[Int] = graph.degrees
        graph.outerJoinVertices(degrees) { (vid, data, deg) => deg.getOrElse(0) }
      },
      vpred = (vid: VertexId, deg: Int) => deg >= 1
    )
    println("***********************")
    graph.vertices.foreach(println(_))
    graph.edges.foreach(println(_))
    println("***********************")
    output.vertices.foreach(println(_))
    output.edges.foreach(println(_))

    var o = Map(1 -> 3) ++ Map(1 -> 4)

    o += (2 -> 4)


    println(o)
    //    o.toIterable.foreach(e => println(e.getClass))

    var s = List(1)
    s :+= 2
    s=5::s
    println( s)
  }

}
