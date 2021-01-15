package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD


object test3 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
    val sc = new SparkContext(conf)
    val edgeInput: RDD[String] = sc.textFile("src/main/resources/edge.csv")
    val vertexInput: RDD[String] = sc.textFile("src/main/resources/vertex.csv")
    val vertext: RDD[(Long, (String, String))] = vertexInput.map(line => {
      val items = line.split(",")
      (items(0).toLong, ("label" -> items(3)))
    }
    )
    val edges: RDD[Edge[None.type]] = edgeInput.map(line => {
      val items = line.split(",")
      Edge(items(0).toLong, items(1).toLong, None)
    })
    val graph = Graph(vertext, edges)
    val out: Graph[VertexId, None.type] = graph.connectedComponents()
    val parts = out.vertices.map(e => e._2).distinct().collect().toList
    //    out.vertices.foreach(println(_))
    val hello = out.vertices.map(e => (e._2, e._1)).groupByKey().map(e => (e._1, e._2.size))
    hello.foreach(println(_))
    println("--------------")
    val unit = out.filter(
      graph => graph,
      vpred = (vid: VertexId, deg: Long) => deg == 1
    )
    //    unit.vertices.foreach(println(_))

    for (i <- parts) {
      val subgraph: Graph[VertexId, None.type] = out.filter(
        graph => graph,
        vpred = (vid: VertexId, part: VertexId) => part == i
      )
      subgraph.edges.map(e => (e.srcId, e.dstId)).repartition(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\outp\\" + i)
    }




  }


}
