package com.zm.compute

import com.zm.tables.T4entity.{SNA_ENTITY_CAR, SNA_ENTITY_CASE, SNA_ENTITY_PERSON, SNA_ENTITY_TEL}
import com.zm.tables.T4rel.{SNA_REL_CAR_CASE, SNA_REL_PERSON_CAR, SNA_REL_PERSON_TEL, SNA_REL_TEL_CASE}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object InitGraph {
  //  def main(args: Array[String]): Unit = {
  //    var ss = SparkSession.builder()
  //      .config("spark.testing.memory", "2147480000")
  //      .appName("InitGraph")
  //      .master("local[2]")
  //      .getOrCreate()
  //
  //    //加载实体数据DF
  //    var person_df = SNA_ENTITY_PERSON.getTable(ss).where("etl_upd_tm='0302'")
  //    var car_df = SNA_ENTITY_CAR.getTable(ss).where("etl_upd_tm='0302'")
  //    var case_df = SNA_ENTITY_CASE.getTable(ss).where("etl_upd_tm='0302'")
  //    var tel_df = SNA_ENTITY_TEL.getTable(ss).where("etl_upd_tm='0302'")
  //
  //    //加载关系数据DF
  //    var person_car_DF = SNA_REL_PERSON_CAR.getTable(ss).where("etl_upd_tm='0302'")
  //    var person_tel_DF = SNA_REL_PERSON_TEL.getTable(ss).where("etl_upd_tm='0302'")
  //    var tel_case_DF = SNA_REL_TEL_CASE.getTable(ss).where("etl_upd_tm='0302'")
  //    var car_case_DF = SNA_REL_CAR_CASE.getTable(ss).where("etl_upd_tm='0302'")
  //    //==========================================================================
  //    //将实体数据DF转成RDD
  //
  //
  //    var person_rdd = person_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "person")))
  //    var car_rdd = car_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "car")))
  //    var case_rdd = case_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "case")))
  //    var tel_rdd = tel_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "tel")))
  //    var vertexes: RDD[(VertexId, Map[String, String])] = person_rdd.union(car_rdd).union(case_rdd).union(car_rdd).union(tel_rdd)
  //
  //    //将关系数据DF转成RDD
  //
  //
  //    var person_car = person_car_DF.rdd.map(row => Edge(row.getAs[String]("person_id").toLong, row.getAs[String]("car_id").toLong, Map("type" -> "own")))
  //    var person_tel = person_tel_DF.rdd.map(row => Edge(row.getAs[String]("person_id").toLong, row.getAs[String]("tel_id").toLong, Map("type" -> "own")))
  //    var tel_case = tel_case_DF.rdd.map(row => Edge(row.getAs[String]("tel_id").toLong, row.getAs[String]("case_id").toLong, Map("type" -> "inform")))
  //    var car_case = car_case_DF.rdd.map(row => Edge(row.getAs[String]("case_id").toLong, row.getAs[String]("car_id").toLong, Map("type" -> "involved")))
  //
  //
  //    val edges: RDD[Edge[Map[String, String]]] = person_car.union(person_tel).union(tel_case).union(car_case)
  //    //创建Graph
  //
  //
  //    val graph: Graph[Map[String, String], Map[String, String]] = Graph(vertexes, edges)
  //    val ccGraph: Graph[VertexId, Map[String, String]] = graph.connectedComponents()
  //
  //
  //    //    ccGraph.vertices.map(row => (row._2, row._1)).aggregateByKey((x, y) => x + y)
  //
  //
  //    //    var parts = ccGraph.vertices.map(e => e._2).distinct().collect()
  //    //    var subGraphs = ArrayBuffer[Graph[VertexId, Map.type]]()
  //    //    var coll = ArrayBuffer[Graph[VertexId, Map[String, String]]]()
  //    //    var col2 = ArrayBuffer[Graph[VertexId, Map[String, String]]]()
  //    //    for (p <- parts) {
  //    //      val subgraph = ccGraph.filter(
  //    //        graph => graph,
  //    //        vpred = (vid: VertexId, part: VertexId) => part == p
  //    //      )
  //    //      if (subgraph.vertices.count() > 1000) {
  //    //        val partedGraph: Graph[VertexId, Map[String, String]] = LabelPropagation.run(subgraph, 100)
  //    //        coll += (partedGraph)
  //    //      } else {
  //    //        col2 += (subgraph)
  //    //      }
  //    //    }
  //    //    var unionRdd: RDD[(VertexId, VertexId)] = null
  //    //
  //    //    for (i <- coll ++ col2) {
  //    //      if (unionRdd == null) {
  //    //        unionRdd = i.vertices
  //    //      } else {
  //    //        unionRdd = unionRdd.union(i.vertices)
  //    //      }
  //    //    }
  //
  //
  //    //根据unionRDD的格式(vertstId,pid)创建DF
  //    var union_row = unionRdd.map(e => Row(e._1, e._2))
  //    val schema1 = StructType(List(
  //      StructField("VERTEX_ID", LongType),
  //      StructField("CLUS_NO", LongType)
  //    ))
  //    var v_c_df = ss.createDataFrame(union_row, schema1)
  //    v_c_df.createTempView("v_c_df")
  //
  //    //
  //    val schema = StructType(
  //      //字段名，字段类型
  //      List(StructField("KEY", StringType, false),
  //        StructField("CLUS_NO", StringType, false),
  //        StructField("VERTEX_ID", StringType, false),
  //        StructField("LABEL", StringType, false),
  //        StructField("TITLE", StringType, true),
  //        StructField("PROPERTIES", StringType, true),
  //        StructField("etl_crt_tm", StringType, false),
  //        StructField("etl_upd_tm", StringType, false)
  //      ))
  //    person_df.createTempView("person_df")
  //    car_df.createTempView("car_df")
  //    case_df.createTempView("case_df")
  //    tel_df.createTempView("tel_df")
  //    ss.sql(
  //      """
  //        |select
  //        |'' as key,
  //        |t2.CLUS_NO as CLUS_NO,
  //        |t2.VERTEX_ID as VERTEX_ID,
  //        |'person'     as label,
  //        |''         as title,
  //        |''         as PROPERTIES,
  //        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')        as etl_crt_tm,
  //        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')         as etl_upd_tm
  //        |from person_df t1 join v_c_df  t2 on t1.MERGE_ID=t2.VERTEX_ID
  //        |
  //        |union all
  //        |select
  //        |'' as key,
  //        |t2.CLUS_NO as CLUS_NO,
  //        |t2.VERTEX_ID as VERTEX_ID,
  //        |'car'     as label,
  //        |''         as title,
  //        |''         as PROPERTIES,
  //        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')        as etl_crt_tm,
  //        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')         as etl_upd_tm
  //        |
  //        |from car_df t1 join v_c_df  t2 on t1.MERGE_ID=t2.VERTEX_ID
  //        |union all
  //        |select
  //        |'' as key,
  //        |t2.CLUS_NO as CLUS_NO,
  //        |t2.VERTEX_ID as VERTEX_ID,
  //        |'case'     as label,
  //        |''         as title,
  //        |''         as PROPERTIES,
  //        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')         as etl_crt_tm,
  //        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')        as etl_upd_tm
  //        |from case_df t1 join v_c_df  t2 on t1.MERGE_ID=t2.VERTEX_ID
  //        |union all
  //        |select
  //        |'' as key,
  //        |t2.CLUS_NO as CLUS_NO,
  //        |t2.VERTEX_ID as VERTEX_ID,
  //        |'tel'     as label,
  //        |''         as title,
  //        |''         as PROPERTIES,
  //        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')         as etl_crt_tm,
  //        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')        as etl_upd_tm
  //        |from tel_df t1 join v_c_df  t2 on t1.MERGE_ID=t2.VERTEX_ID
  //        |
  //      """.stripMargin).show(100)
  //
  //
  //    //    var row = unionRdd.map(e => Row(e._1, e._2))
  //    //    val reDF: DataFrame = ss.createDataFrame(row, schema)
  //    //    reDF.createTempView("SNA_CLUSTER_VERTEX")
  //    //    reDF.write
  //
  //
  //    //===============================
  //    //    for (subgraph <- subGraphs) {
  //    //      if (subgraph.vertices.count() > 1000) {
  //    //        val partedGraph: Graph[VertexId, None.type] = LabelPropagation.run(subgraph, 100)
  //    //        partedGraph.vertices.saveAsTextFile("C:\\Users\\zm\\Desktop\\communityDetect\\")
  //    //      } else {
  //    //      }
  //    //    }
  //  }
}
