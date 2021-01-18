package com.LPA

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LPZhu {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("LPA").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val edges: RDD[Edge[Double]] = sc.parallelize(Array(
      Edge(1L, 2L, 1.0),
      Edge(2L, 1L, 3.0),
      Edge(3L, 4L, 1.0),
      Edge(4L, 5L, 1.0),
      Edge(1L, 5L, 2.0),
      Edge(5L, 2L, 1.0)
    ))

    val users: RDD[(Long, String, Double)] = sc.parallelize(Array(
      (1L, "1", 0.5),
      (1L, "2", 0.3),
      (1L, "3", 0.2)
    ))
    val labelsArr: Array[String] = users.map(e => (e._2, 1)).reduceByKey(_ + _).map(_._1).collect()
    val verts: RDD[(Long, LPVertex)] = users.map(x => (x._1, (x._2, x._3))).groupByKey().map(x => {
      val vert = new LPVertex
      vert.isSeedNode = true
      vert.injectedLabels = x._2.toMap
      (x._1, vert)
    }
    )
    var graph1 = Graph.fromEdges(edges, 1)
    //    graph.vertices.foreach(println(_))
    graph1.edges.foreach(println(_))
    println("---------------")
    verts.foreach(println(_))
    println("---------------")
    val graph: Graph[LPVertex, Double] = graph1.outerJoinVertices(verts) { (vid, vdata, vert) =>
      if (vert eq None) {
        val v = new LPVertex
        v
      } else vert.get
    }
    val lp = new LPZhuGhan(graph, undirected = true, labels = labelsArr)
    lp.runUntilConvergence(0.0001)
  }

}
