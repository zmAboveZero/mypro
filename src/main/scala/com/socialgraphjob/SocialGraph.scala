package com.socialgraphjob

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, PartitionID, VertexId}

class SocialGraph(sc: SparkContext) {

  def connectedComponent = {
    graph.connectedComponents vertices
  }

  def connectedComponentGroupedByUsers = {
    verts.join(connectedComponent).map{
      case (_,(username,comp))=>(username,comp)
    }
  }


  def getBFS(root: VertexId) = {
    val initiaGraph: Graph[Double, PartitionID] = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)
    val bfs: Graph[Double, PartitionID] = initiaGraph.pregel(Double.PositiveInfinity, maxIterations = 10)(
      (_, attr, msg) => math.min(attr, msg),
      triplet => {
        if (triplet.srcAttr != Double.PositiveInfinity) {
          Iterator((triplet.srcId, triplet.srcAttr + 1))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)).cache()
    bfs
  }

  def degreeOfSeparationSingleUser(root: VertexId) = {
    getBFS(root).vertices. join(verts).take(10)
  }


  def verts = sc.textFile(USER_NAMES).flatMap(InputDataFlow.parseNames)

  def edges = sc.textFile(USER_GRAPH).flatMap(InputDataFlow.makeEdges)

  def graph = Graph(verts, edges).cache()

  def getMostConnectedUsers(amount: Int) = {
    graph.degrees.join(verts) sortBy( {
      case (_, (userName, _)) => userName
    }, ascending = false) take (amount)
  }


  type ConnectUser = (PartitionID, String)
  type DegreeeOfSeperation = (Double, String)

}
