package com.zm.test

import com.zm.tables.T4entity.{SNA_ENTITY_CAR, SNA_ENTITY_CASE, SNA_ENTITY_PERSON, SNA_ENTITY_TEL}
import com.zm.tables.T4rel.{SNA_REL_CAR_CASE, SNA_REL_PERSON_CAR, SNA_REL_PERSON_TEL, SNA_REL_TEL_CASE}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object Test7 {
  def main(args: Array[String]): Unit = {
    var ss = SparkSession.builder()
      .config("spark.testing.memory", "2147480000")
      .appName("InitGraph")
      .master("local[2]")
      .getOrCreate()


    //加载实体数据DF
    var person_df = SNA_ENTITY_PERSON.getTable(ss).where("etl_upd_tm='0302'")
    var car_df = SNA_ENTITY_CAR.getTable(ss).where("etl_upd_tm='0302'")
    var case_df = SNA_ENTITY_CASE.getTable(ss).where("etl_upd_tm='0302'")
    var tel_df = SNA_ENTITY_TEL.getTable(ss).where("etl_upd_tm='0302'")

    //加载关系数据DF
    var person_car_DF = SNA_REL_PERSON_CAR.getTable(ss).where("etl_upd_tm='0302'")
    var person_tel_DF = SNA_REL_PERSON_TEL.getTable(ss).where("etl_upd_tm='0302'")
    var tel_case_DF = SNA_REL_TEL_CASE.getTable(ss).where("etl_upd_tm='0302'")
    var car_case_DF = SNA_REL_CAR_CASE.getTable(ss).where("etl_upd_tm='0302'")
    //==========================================================================
    //将实体数据DF转成RDD


    var person_rdd = person_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "person")))
    var car_rdd = car_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "car")))
    var case_rdd = case_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "case")))
    var tel_rdd = tel_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "tel")))


    var vertexes: RDD[(VertexId, Map[String, String])] = person_rdd.union(car_rdd).union(case_rdd).union(car_rdd).union(tel_rdd)

    //将关系数据DF转成RDD


    var person_car = person_car_DF.rdd.map(row => Edge(row.getAs[String]("person_id").toLong, row.getAs[String]("car_id").toLong, Map("type" -> "own")))
    var person_tel = person_tel_DF.rdd.map(row => Edge(row.getAs[String]("person_id").toLong, row.getAs[String]("tel_id").toLong, Map("type" -> "own")))
    var tel_case = tel_case_DF.rdd.map(row => Edge(row.getAs[String]("tel_id").toLong, row.getAs[String]("case_id").toLong, Map("type" -> "inform")))
    var car_case = car_case_DF.rdd.map(row => Edge(row.getAs[String]("case_id").toLong, row.getAs[String]("car_id").toLong, Map("type" -> "involved")))


    val edges: RDD[Edge[Map[String, String]]] = person_car.union(person_tel).union(tel_case).union(car_case)
    //创建Graph
    val graph: Graph[Map[String, String], Map[String, String]] = Graph(vertexes, edges)
    val ccGraph: Graph[VertexId, Map[String, String]] = graph.connectedComponents()

    val parts = ccGraph.vertices.map(e => e._2).distinct(1).collect()

    var subGraphs = ArrayBuffer[Graph[VertexId, Map.type]]()
    var coll = ArrayBuffer[Graph[VertexId, Map[String, String]]]()
    var col2 = ArrayBuffer[Graph[VertexId, Map[String, String]]]()
    for (p <- parts) {
      val subgraph = ccGraph.filter(
        graph => graph,
        vpred = (vid: VertexId, part: VertexId) => part == p
      )
      if (subgraph.vertices.count() > 1000) {
        val partedGraph: Graph[VertexId, Map[String, String]] = LabelPropagation.run(subgraph, 100)
        coll += (partedGraph)
      } else {
        col2 += (subgraph)
      }
    }


    for (i <- col2) {
      println(System.currentTimeMillis().formatted("yyyy-MM-DD HH:MM:SS"))
      i.pageRank(0.15)
    }



    //    println(col2.length)
    //    val graph_rdd: RDD[Graph[VertexId, Map[String, String]]] = ss.sparkContext.parallelize(col2)
    //    graph_rdd.map(e => e.numVertices).foreach(println(_))
    //    val hello: RDD[Graph[Double, Double]] = graph_rdd.map(e => e.pageRank(0.15, 0.15))
    //    var input1 = ss.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6))
    //    var input2 = ss.sparkContext.parallelize(List(7, 8, 9, 10, 11, 12))
    //    input1.map(e => e == input2.max()).foreach(println(_))
    //    var m = input2.max()
    //    println("=====")
    //    input1.map(e => e == input2.max()).foreach(println(_))
    //    println(input2.max())
  }

}
