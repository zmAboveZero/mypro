package com.zm.compute

import com.zm.tables.T4entity.{SNA_ENTITY_CAR, SNA_ENTITY_CASE, SNA_ENTITY_PERSON, SNA_ENTITY_TEL}
import com.zm.tables.T4rel.{SNA_REL_CAR_CASE, SNA_REL_PERSON_CAR, SNA_REL_PERSON_TEL, SNA_REL_TEL_CASE}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object InitGraph {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .config("spark.testing.memory", "2147480000")
      .appName("InitGraph")
      .master("local[2]")
      .getOrCreate()
    import ss.implicits._
    //加载实体数据DF
    //输入person_df，car_df，case_df，tel_df
    //输出vertextes
    var person_df = SNA_ENTITY_PERSON.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("person"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0302'")
    var car_df = SNA_ENTITY_CAR.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("car"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0302'")
    var case_df = SNA_ENTITY_CASE.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("case"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0302'")
    var tel_df = SNA_ENTITY_TEL.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("tel"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0302'")
    var vertextes = person_df.union(car_df).union(case_df).union(tel_df)
    vertextes.createTempView("vertextes")
    //==========================================================================
    //将实体数据DF转成RDD
    //    var person_rdd = person_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "person")))
    //    var car_rdd = car_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "car")))
    //    var case_rdd = case_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "case")))
    //    var tel_rdd = tel_df.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, Map("label" -> "tel")))
    //    var vertexes: RDD[(VertexId, Map[String, String])] = person_rdd.union(car_rdd).union(case_rdd).union(car_rdd).union(tel_rdd)

    //将关系数据DF转成RDD


    //加载关系数据DF
    //输入person_car_DF，person_tel_DF，tel_case_DF，car_case_DF
    //输出edges
    var person_car_DF = SNA_REL_PERSON_CAR.getTable(ss).select("person_id", "car_id", "rel_type").where("etl_upd_tm='0302'")
    var person_tel_DF = SNA_REL_PERSON_TEL.getTable(ss).select("person_id", "tel_id", "rel_type").where("etl_upd_tm='0302'")
    var tel_case_DF = SNA_REL_TEL_CASE.getTable(ss).select("tel_id", "case_id", "rel_type").where("etl_upd_tm='0302'")
    var car_case_DF = SNA_REL_CAR_CASE.getTable(ss).select("car_id", "case_id", "rel_type").where("etl_upd_tm='0302'")
    person_car_DF.union(person_tel_DF).union(tel_case_DF).union(car_case_DF).createTempView("t_edges")


    var edges = ss.sql(
      """
        |select
        |person_id as srcID,
        |car_id  as destID,
        |rel_type as LABEL
        |from t_edges
      """.stripMargin)
    edges.createTempView("edges")
    edges.show()


    //根据上面的节点数据和边数据，创建图，并连同分量计算
    //输入vertextes，edges
    //输出

    var Graph_Vertexes = vertextes.rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, None))
    var Graph_Edges = edges.rdd.map(row => Edge(row.getAs[String]("srcID").toLong, row.getAs[String]("destID").toLong, None))
    val graph = Graph(Graph_Vertexes, Graph_Edges)
    val ccGraph = graph.connectedComponents()

    //有已经分区过的图，进行二次划分
    //输入new_ccgraph
    //输出reRDD格式("VERTEX_ID", "CLUS_NO")
    var waitingsplit = ccGraph.vertices.map(e => (e._2, 1)).reduceByKey((x, y) => x + y).filter(e => e._2 > 1000)
    var waitingsplitArr = waitingsplit.map((_._1)).collect()
    var subgraph = ccGraph.filter(
      graph => graph,
      vpred = (vid: VertexId, part: VertexId) => waitingsplitArr.contains(part)
    )
    val sc = ss.sparkContext
    var subgraphV: RDD[(VertexId, VertexId)] = null
    for (elem <- waitingsplitArr) {
      subgraphV = sc.union(LabelPropagation.run(subgraph.filter(
        graph => graph,
        vpred = (vid: VertexId, part: VertexId) => part == elem
      ), 100).vertices)
    }
    var reRDD: RDD[(VertexId, VertexId)] = null
    if (subgraphV == null) {
      reRDD = ccGraph.vertices
    } else {
      reRDD = ccGraph.vertices.leftOuterJoin(subgraphV).map(e => (e._1, e._2._2.getOrElse(e._2._1)))
    }


    //补充详细信息
    //输入reRDD，vertextes
    //输出full_v
    //今天的最终实体数据full_v
    var reRDDDF = reRDD.toDF("VERTEX_ID", "CLUS_NO")
    reRDDDF.createTempView("t_reRDDDF")
    var full_v = ss.sql(
      """
        |select
        | '' as key,
        |t1.CLUS_NO as CLUS_NO,
        |t1.VERTEX_ID as VERTEX_ID,
        |t2.label    as label,
        |'' as title,
        |t2.PROPERTIES  as PROPERTIES,
        |'' as etl_crt_tm,
        |'' as etl_upd_tm
        |from t_reRDDDF t1
        |join vertextes t2
        |on t1.VERTEX_ID =t2.MERGE_ID
        |
      """.stripMargin)

    full_v.createTempView("full_v")

    //补全今天的关系数据，然后得出今天的最终关系数据
    //输入edges，full_v
    //输出re_edges
    //最终的关系数据re_edges
    edges.show()
    full_v.show()
    ss.sql(
      """
        |select
        | ''  as KEY,
        | t2.CLUS_NO    as  SOURCE_CLUS_NO,
        | t3.CLUS_NO    as  TARGET_CLUS_NO,
        | '' as LABEL,
        | '' as TITLE,
        | t1.srcId as SOURCE_ID,
        | t2.label as SOURCE_LABEL,
        | t1.destID as TARGET_ID,
        | t3.label as TARGET_LABEL,
        | ''  as PROPERTIES,
        | ''  as etl_crt_tm,
        | ''  as etl_upd_tm
        |from edges t1
        |join full_v t2
        |on t1.srcId=t2.VERTEX_ID
        |join full_v t3
        |on t1.destID=t3.VERTEX_ID
        |
        |
          """.stripMargin).show()


    //    full_v
    //    edges
    //      .map(e => (e.srcId, e.dstId, e.attr("type")))
    //      .toDF("srcId", "dstId", "label")
    //      //      .show()
    //      .createTempView("edges")
    //    ss.sql(
    //      """
    //        |
    //        |select * from (
    //        |select
    //        |     ''  as KEY,
    //        |     ''  as CLUS_NO,
    //        |    t2.CLUS_NO    as  SOURCE_CLUS_NO,
    //        |    t2.CLUS_NO    as  TARGET_CLUS_NO,
    //        |	   t1.label  as LABEL,
    //        |    ''     as TITLE,
    //        |    t1.srcId  as  SOURCE_ID,
    //        |	   t2.label as SOURCE_LABEL,
    //        |    t1.dstId  as  TARGET_ID,
    //        |	   t2.label  as  TARGET_LABEL,
    //        |    ''  as PROPERTIES,
    //        |    ''  as etl_crt_tm,
    //        |    ''  as etl_upd_tm
    //        |   from edges t1
    //        |   left join SNA_CLUSTER_VERTEX t2
    //        |   on t1.srcId=t2.VERTEX_ID
    //        |   left join SNA_CLUSTER_VERTEX t3
    //        |   on t1.dstId=t3.VERTEX_ID
    //        |   ) temp where SOURCE_CLUS_NO=TARGET_CLUS_NO
    //        |
    //      """.stripMargin).show(100)
    //
    //    //===============================
    //    //    val edges: RDD[Edge[Map[String, String]]] = edges
    //    //    var row = unionRdd.map(e => Row(e._1, e._2))
    //    //    val reDF: DataFrame = ss.createDataFrame(row, schema)
    //    //    reDF.createTempView("SNA_CLUSTER_VERTEX")
    //    //    reDF.write
    //
    //    //    for (subgraph <- subGraphs) {
    //    //      if (subgraph.vertices.count() > 1000) {
    //    //        val partedGraph: Graph[VertexId, None.type] = LabelPropagation.run(subgraph, 100)
    //    //        partedGraph.vertices.saveAsTextFile("C:\\Users\\zm\\Desktop\\communityDetect\\")
    //    //      } else {
    //    //      }
    //    //    }
  }
}
