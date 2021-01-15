package zm1

/**
  * Created by mac on 3/13/16.
  */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import breeze.plot._
object PlotDegreeDistribution extends App {

  /**
    * configuration of spark
    */
  val conf = new SparkConf().set("spark.testing.memory", "2147480000").setAppName("PeopleCorrelationAnalysis")
  conf.setMaster("local[*]")
  val sc = new SparkContext(conf)
//  sc.addJar("/Users/mac/Documents/GraphXSurvey/SparkTest/out/artifacts/SparkTest_jar4/SparkTest.jar")

  case class Person(name:String, sex:String)
  case class Link(relationship:String, happenDate:String)
  /**
    * create a graph from files which have specified form
    * @param vertexFilePath file path of vertexs.csv
    * @param edgeFilePath file path of edges.csv
    * @return
    */
  def createGraph(vertexFilePath:String, edgeFilePath:String): Graph[Person,Link] ={

    val vertices = sc.textFile(vertexFilePath)
    val links= sc.textFile(edgeFilePath)
    //构建边、顶点RDD

    val verticesRDD: RDD[(VertexId,Person)] = vertices map {line
    =>
      val row = line split ','
      (row(0).toLong,Person(row(1),row(2)))
    }

    val linksRDD: RDD[Edge[Link]] = links map { line =>
      val row = line split ','
      Edge(row(0).toLong, row(1).toLong, Link("df", " "))
    }

    //构建图
    val social: Graph[Person,Link] = Graph(verticesRDD, linksRDD)
    return social

  }
  /**
    * the main graph
    */
  var social:Graph[Person,Link] = createGraph("C:\\Users\\zm\\Desktop\\graphx\\testttt\\src\\main\\resources\\vertexs.csv", "C:\\Users\\zm\\Desktop\\graphx\\testttt\\src\\main\\resources\\edges.csv")
  social.cache()

  /**
    * obtain the degree distribution in a Histogram
    * @param net
    * @return
    */
  def degreeHistogram(net: Graph[Person, Link]): Array[(Int, Int)] =
    net.degrees.map(t => (t._2,t._1)).
      groupByKey.map(t => (t._1,t._2.size)).
      sortBy(_._1).collect()

  val nn = social.numVertices
  val degreeDistribution: Array[(PartitionID, Double)] = degreeHistogram(social).map({case(d,n) => (d,n.toDouble/nn)})
  println(degreeDistribution.mkString)
  val f = Figure()
  val p1 = f.subplot(2,1,0)
  var degreeDistribution1=Array((1L,2L),(2L,3L))
  val x = new DenseVector(degreeDistribution map (_._1.toDouble))
  val y = new DenseVector(degreeDistribution map (_._2))


  p1.xlabel = "Degrees"
  p1.ylabel = "Distribution"
  p1 += plot(x, y)
  p1.title = "Degree distribution of network"
  val p2 = f.subplot(2,1,1)
  val egoDegrees = social.degrees.map(_._2).collect()
  p2.xlabel = "Degrees"
  p2.ylabel = "Histogram of node degrees"
  p2 += hist(egoDegrees, 10)

}
