package com.GraphComputer

import com.GraphComputer.utils.ImportData
import org.apache.spark.graphx
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


object Main {
  def main(args: Array[String]): Unit = {
    val vetexes = ImportData.importVetexes()
    val edges: RDD[Edge[String]] = ImportData.importEdges()
    val graph = Graph(vetexes, edges)
    val ccGraph: Graph[VertexId, String] = graph.connectedComponents()
    val parts = ccGraph.vertices.map(e => e._2).distinct().collect()
    var subgraphs = ArrayBuffer[Graph[graphx.VertexId, String]]()


    edges.zipWithUniqueId()

    for (i <- parts) {
      val subgraph = ccGraph.filter(
        graph => graph,
        vpred = (vid: VertexId, part: VertexId) => part == i
      )
      subgraphs += subgraph
      subgraph.edges.map(e => (e.srcId, e.dstId)).repartition(1).saveAsTextFile("C:\\Users\\zm\\Desktop\\community\\" + i)
    }


    for (subgraph <- subgraphs) {
      if (subgraph.vertices.count() > 1000) {
        val partedGraph: Graph[VertexId, String] = LabelPropagation.run(subgraph, 100)
        partedGraph.vertices.saveAsTextFile("C:\\Users\\zm\\Desktop\\communityDetect\\")
      }
    }


  }
}
