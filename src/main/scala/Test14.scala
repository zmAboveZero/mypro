import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test14 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
    val sc = new SparkContext(conf)
    val edges = sc.parallelize(List((1, 2), (2, 3), (3, 4), (4, 5), (2, 5), (3, 6), (3, 6),(7,8),(9,10))).map(e => Edge(e._1, e._2, None))
    println(edges.count())
    val graph = Graph.fromEdges(edges, None)
    println(graph.edges.count())
    graph.connectedComponents().vertices.foreach(ele=>println(ele._1,ele._2))

  }

}
