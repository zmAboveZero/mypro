package zm1

import java.awt.Color

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}
import java.lang.Double

import org.apache.log4j.{Level, Logger}

import scala.runtime.RichInt

/**
  * Created by mac on 3/31/16.
  */
object ImportanceEvaluation extends App {
  /**
    * configuration of spark
    */
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().set("spark.testing.memory", "2147480000").setAppName("PeopleCorrelationAnalysis")
  //  conf.setMaster("spark://MacdeMacBook-Pro-3.local:7077")
  conf.setMaster("local[*]")
  val sc = new SparkContext(conf)

  //  sc.addJar("/Users/mac/Documents/GraphXSurvey/SparkTest/out/artifacts/SparkTest_jar/SparkTest.jar")

  case class Person(name: String, sex: String)

  case class Link(relationship: String, happenDate: String)

  /**
    * create a graph from files which have specified form
    *
    * @param vertexFilePath file path of vertexs.csv
    * @param edgeFilePath   file path of edges.csv
    * @return
    */
  def createGraph(vertexFilePath: String, edgeFilePath: String): Graph[Person, Link] = {

    val vertices = sc.textFile(vertexFilePath,1)
    val links = sc.textFile(edgeFilePath,1)
    //构建边、顶点RDD

    val verticesRDD: RDD[(VertexId, Person)] = vertices map { line =>
      val row = line split ','
      (row(0).toLong, Person(row(1), row(2)))
    }

    val linksRDD: RDD[Edge[Link]] = links map { line =>
      val row = line split ','
      Edge(row(0).toLong, row(1).toLong, Link("df", " "))
    }

    /**
      * create a graph from files which have specified form
      *
      * @param vertexFilePath file path of vertexs.csv
      * @param edgeFilePath   file path of edges.csv
      * @return
      */


    //构建图
    val social: Graph[Person, Link] = Graph(verticesRDD, linksRDD)
    return social

  }

  /**
    * the main graph
    */
  var graph: Graph[Person, Link] = createGraph("C:\\Users\\zm\\Desktop\\graphx\\testttt\\src\\main\\resources\\vertexs.csv", "C:\\Users\\zm\\Desktop\\graphx\\testttt\\src\\main\\resources\\edges.csv")
  graph.cache()


  /**
    * Start of Main Part
    */
  def multiTen(a: Double): Int = {
    (a * 10).toInt
  }

//  val tests = graph.pageRank(0.0001).vertices
//  tests.sortBy(_._2).foreach(println(_))
  val ranks = graph.pageRank(0.0001).vertices.sortBy(_._2, false) //false表示降序排列
  println("========顶点的PageRank值(降序排列)")

  ranks.foreach(x => println("顶点ID:" + x._1 + "  PR值:" + x._2))
  val ranksTen = ranks.map(x => (x._1, (x._2 * 10).toInt))


  /**
    * Visualize
    */
  val graphStream: SingleGraph = new SingleGraph("ImportanceEvaluation");

  // Set up the visual attributes for graph visualization
  graphStream.addAttribute("ui.stylesheet", "url(C:\\Users\\zm\\Desktop\\graphx\\testttt\\src\\main\\scala\\zm\\style\\stylesheet.css)")
  graphStream.addAttribute("ui.quality") //to enable slower but better rendering.
  graphStream.addAttribute("ui.antialias") //to enable anti-aliasing(抗锯齿或边缘柔化) of shapes drawn by the viewer

  // Given the egoNetwork, load the graphX vertices into GraphStream
  for ((id, person: Person) <- graph.vertices.collect()) {
    val node = graphStream.addNode(id.toString).asInstanceOf[SingleNode]
    node.addAttribute("ui.label", person.name + ":" + ranks.lookup(id)(0))
    val x: java.lang.Double = ranks.lookup(id)(0) * 0.7
    node.setAttribute("ui.color", x)
  }
  // Load the graphX edges into GraphStream edges
  //    graph.edges.collect().foreach(println)
  for (Edge(x, y, link: Link) <- graph.edges.collect()) {
    val edge = graphStream.addEdge(x.toString ++ y.toString,
      x.toString, y.toString,
      true).
      asInstanceOf[AbstractEdge]
    edge.addAttribute("ui.label", link.relationship)
  }

//  graphStream.display()


  /**
    * End of  Main Part
    */

  //  /**
  //    * Run PageRank with an error tolerance of 0.0001,and
  //    * get the top 10 vertices
  //    */
  //  def getTop10Vertices() ={
  //    val ranks = social.pageRank(0.0001).vertices.sortBy(_._2,false).take(10)
  //    ranks.foreach(x => println(x._1+" "+x._2))
  //    ranks
  //  }

  //  def sortByImportance(graph:Graph[Person,Link], tol:Double = 0.0001) : Array[(VertexId,Person)]= {
  //
  //


}
