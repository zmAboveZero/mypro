package com.LPA

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

class VertexInitialize() extends  java.io.Serializable{
  private val random = new java.util.Random()

  def normalize(m: Map[String, Double]) = {
    val sum: Double = m.values.sum
    if (sum > 0) {
      m.map(x => (x._1, x._2 / sum))
    } else m
  }

  def apply(g: Graph[LPVertex, Double], labels: Array[String]) = {
    val verts: RDD[(VertexId, LPVertex)] = g.vertices.map({ case (vid, vdata) =>
      vdata.injectedLabels = normalize(vdata.injectedLabels)
      if (vdata.isSeedNode) {
        vdata.estimatedLabels = vdata.injectedLabels
      } else {
        for (label <- labels) {
          vdata.estimatedLabels += (label -> random.nextDouble)
        }
      }
      (vid, vdata)
    })
    verts
  }
}
