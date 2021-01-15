package P01pathFindingAndGraphSearch.ImportData

import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}

object ImportData {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
    val sc = new SparkContext(conf)
    val nodesRDD: RDD[String] = sc.textFile("src/main/resources/transport-nodes.csv", 1)
    val withid: RDD[(String, Long)] = nodesRDD.map(line => line.split(",")(0)).zipWithUniqueId()

    val rel: RDD[String] = sc.textFile("src/main/resources/transport-relationships.csv", 1)
    val relid: RDD[(String, String)] = rel.map({ line =>
      val items = line.split(",")
      val start = items(0)
      val end = items(1)
      (start, end)
    }
    )
    rel.collect()
    val reli: RDD[(Long, Long)] = withid.join(relid).map(e => (e._2._2, e._2._1)).join(withid).map(_._2)
    val vertex: RDD[(Long, String)] = withid.map(e => (e._2, e._1))
    val edges: RDD[Edge[None.type]] = reli.map(e => Edge(e._1, e._2, None))
    val graph = Graph(vertex, edges)
    graph.inDegrees
    ShortestPaths.run(graph, Array(8L, 3L, 1L)).vertices.foreach(println(_))


    val graphStream: SingleGraph = new SingleGraph("P03CommunityDetection");
    // Set up the visual attributes for graph visualization
    graphStream.addAttribute("ui.stylesheet", "url(C:\\Users\\zm\\Desktop\\graphx\\testttt\\src\\main\\scala\\zm\\style\\stylesheet.css)")
    graphStream.addAttribute("ui.quality") //to enable slower but better rendering.
    graphStream.addAttribute("ui.antialias") //to enable anti-aliasing(抗锯齿或边缘柔化) of shapes drawn by the viewer
    // load the graphX vertices into GraphStream
    for ((id, name) <- graph.vertices.collect()) {
      val node = graphStream.addNode(id.toString).asInstanceOf[SingleNode]
      node.addAttribute("ui.label", id + "")
    }
    //    for ((id, name) <- graph.vertices.collect()) {
    //      val node = graphStream.getNode(id.toString).asInstanceOf[SingleNode]
    //      val x: java.lang.Double = (label - min) / delt.toDouble
    //      node.setAttribute("ui.color", x)
    //    }
    //    graph.edges.collect().foreach(println(_))
    for (Edge(x, y, link) <- graph.edges.collect()) {
      val edge = graphStream.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
    }
    graphStream.display()
  }
}
