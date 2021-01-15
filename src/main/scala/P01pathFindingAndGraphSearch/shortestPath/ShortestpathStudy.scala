package P01pathFindingAndGraphSearch.shortestPath

import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}

object ShortestpathStudy {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
    val sc = new SparkContext(conf)
    val edges: RDD[(Long, Long)] = sc.parallelize(Array((1L, 2L), (2L, 3L), (3L, 4L), (1L, 5L), (5L, 4L)))

    val value: RDD[Edge[None.type]] = edges.map(e =>
      Edge(e._1, e._2, None)
    )
    val graph: Graph[None.type, None.type] = Graph.fromEdges(value, None)
    val sgraph = ShortestPaths.run(graph, Array(1L, 4L))
    sgraph.vertices.foreach(println(_))
    val graphStream: SingleGraph = new SingleGraph("P03CommunityDetection");
    graphStream.addAttribute("ui.stylesheet", "url(C:\\Users\\zm\\Desktop\\graphx\\testttt\\src\\main\\scala\\zm\\style\\stylesheet.css)")
    graphStream.addAttribute("ui.quality") //to enable slower but better rendering.
    graphStream.addAttribute("ui.antialias") //to enable anti-aliasing(抗锯齿或边缘柔化) of shapes drawn by the viewer
    // load the graphX vertices into GraphStream
    for ((id, name) <- sgraph.vertices.collect()) {
      val node = graphStream.addNode(id.toString).asInstanceOf[SingleNode]
      node.addAttribute("ui.label", id+"")
    }


    println(sgraph.vertices.collect().toList)
    for (Edge(x, y, link) <- graph.edges.collect()) {
      val edge = graphStream.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
    }
    graphStream.display()


  }

}
