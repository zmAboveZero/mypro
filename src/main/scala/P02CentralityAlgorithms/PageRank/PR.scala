package P02CentralityAlgorithms.PageRank

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object PR {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
    val sc = new SparkContext(conf)
    val edges: RDD[(Long, Long)] = sc.parallelize(Array((1L, 4L), (2L, 4L), (3L, 4L), (4L, 5L), (5L, 6L)))
    val value: RDD[Edge[None.type]] = edges.map(e =>
      Edge(e._1, e._2, None)
    )
    val graph: Graph[None.type, None.type] = Graph.fromEdges(value, None)
    graph.pageRank(0.1).vertices.collect().foreach(println(_))
    val re: VertexRDD[Array[Edge[None.type]]] = graph.collectEdges(EdgeDirection.Out)
    val re_e: Array[(VertexId, Array[Edge[None.type]])] = re.collect()

    for (elem <- re_e) {
      elem._2.foreach(println(_))
    }
  }

}
