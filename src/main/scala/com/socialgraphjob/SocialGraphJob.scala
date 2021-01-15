package com.socialgraphjob

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SocialGraphJob {
  def main(args: Array[String]): Unit = {
    Logger getLogger ("org") setLevel (Level.ERROR)
    val conf: SparkConf = new SparkConf() setAppName ("test") setMaster ("local[*]")
    val sc = new SparkContext(conf)
    val graph = new SocialGraph(sc)
    //    println("Top 10 most-connected users:")
    //    graph getMostConnectedUsers(10)foreach println
    println("Compute degree of separation for user Arch")
    //    graph.degreeOfSeparationSingleUser(5306)foreach println
    //    val unit: Unit = (1, 2, 3, 4, 5, 65, 6, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9)
    //    val tuple = (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    //    println(Iterator(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1).length)
    //    Iterator(unit)
    //    Iterator(tuple).foreach(println(_))
    //    assert(1 == 2)
    //    println("fdsf")
    println("Connected componet")
    //    graph.connectedComponentGroupedByUsers
    //      .sortBy({ case (_, lowestVertexId) => lowestVertexId }, ascending = false)
    //      .take(10) foreach (println(_))
    //    graph.graph.triplets.take(10).foreach(e=>println(e.srcAttr))
    graph.graph.triplets.take(10).foreach(e => println(e.dstAttr))

    //    graph.verts.take(10).foreach(println(_))

    //    println(Some(999))
    //    val someInt = Some(999)
    //    Some(999).get
  }


}
