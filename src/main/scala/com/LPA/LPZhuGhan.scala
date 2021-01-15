package com.LPA

import org.apache.spark.graphx.Graph

class LPZhuGhan(graph: Graph[LPVertex, Double], self_weight: Double = 1.0, undirected: Boolean = false, labels: Array[String]) {
  def runUntilConvergence(tol: Double) = {
    val initializer = new VertexInitialize()
    val verts = initializer(graph, labels)


  }


}
