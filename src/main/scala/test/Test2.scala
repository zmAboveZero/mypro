package test

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}

object Test2 {
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
    val labels: RDD[String] = graph.vertices.map(e => e._2._2).distinct()
    val code: Map[String, Double] = Map(("person", 1.0), ("policy", 2.0), ("car", 4.0), ("case", 8.0), ("policy", 16.0))
    //    labels.foreach(println(_))


    val graphStream = new SingleGraph("test2")
    graphStream.addAttribute("ui.stylesheet", "url(C:\\Users\\zm\\Desktop\\graphx\\testttt\\src\\main\\scala\\zm\\style\\stylesheet.css)")
    graphStream.addAttribute("ui.quality") //to enable slower but better rendering.
    graphStream.addAttribute("ui.antialias") //to enable anti-aliasing(抗锯齿或边缘柔化) of shapes drawn by the viewer
    var i = 1
    for (e <- graph.vertices.collect()) {
      val node = graphStream.addNode(e._1.toString).asInstanceOf[SingleNode]
      //      node.addAttribute("ui.label", e._1 + "")
      //      val co: java.labelng.Double = ((i % 3) * 3).toDouble
      //      val coo: java.lang.Double = 0.58
      //      println(co)
      //      val  haha=Double()

      val co: java.lang.Double = 2 * code(e._2._2) / 31
      //      println(co)
      node.setAttribute("ui.color", co)
    }

    for (e <- graph.edges.collect()) {
      graphStream.addEdge(e.srcId.toString ++ e.dstId.toString, e.srcId + "", e.dstId + "", true).asInstanceOf[AbstractEdge]
    }
    //    graphStream.display()


    val subgrap: Graph[(String, String), None.type] = graph.subgraph(vpred = (id, attr) => id > 35)
    val graphStream2 = new SingleGraph("test2")
    graphStream2.addAttribute("ui.stylesheet", "url(C:\\Users\\zm\\Desktop\\graphx\\testttt\\src\\main\\scala\\zm\\style\\stylesheet.css)")
    graphStream2.addAttribute("ui.quality") //to enable slower but better rendering.
    graphStream2.addAttribute("ui.antialias") //to enable anti-aliasing(抗锯齿或边缘柔化) of shapes drawn by the viewer
    for (e <- subgrap.vertices.collect()) {
      val node = graphStream2.addNode(e._1.toString).asInstanceOf[SingleNode]
      val co: java.lang.Double = 2 * code(e._2._2) / 31
      //      println(co)
      node.setAttribute("ui.color", co)
    }
    for (e <- subgrap.edges.collect()) {
      graphStream2.addEdge(e.srcId.toString ++ e.dstId.toString, e.srcId + "", e.dstId + "", true).asInstanceOf[AbstractEdge]
    }
    //    graphStream2.display()
    //    println(graph.vertices.filter(v => v._2._2.equals("person")).count())
    //    graph.vertices.foreach(println(_))

    val srcvet: VertexRDD[(String, String)] = graph.vertices.filter(v => v._2._2.equals("person"))
    val out = graph.outerJoinVertices(srcvet)((vid, data, dattr) => {
      val r = dattr match {
        case Some(data) => Map(data, ("person" -> 1))
        case None => Map(data)
      }
      r
      //      dattr
    }
    )
    out.vertices.foreach(println(_))




    //    srcvet.foreach(e => println(e._2._2))


  }

}
