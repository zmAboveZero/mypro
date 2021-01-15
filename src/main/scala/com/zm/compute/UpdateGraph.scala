package com.zm.compute

import com.zm.tables.T4entity.{SNA_ENTITY_CAR, SNA_ENTITY_CASE, SNA_ENTITY_PERSON, SNA_ENTITY_TEL}
import com.zm.tables.T4rel.{SNA_REL_CAR_CASE, SNA_REL_PERSON_CAR, SNA_REL_PERSON_TEL, SNA_REL_TEL_CASE}
import com.zm.tables.T5InitialGraph.{SNA_CLUSTER_EDGE, SNA_CLUSTER_VERTEX}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object UpdateGraph {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .config("spark.testing.memory", "2147480000")
      .appName("InitGraph")
      .master("local[2]")
      .getOrCreate()

    import ss.implicits._

    //获取昨天的实体数据
    //输入person_df_T，car_df_T，case_df_T，tel_df_T
    //输出df_T

    var person_df_T = SNA_ENTITY_PERSON.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("person"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0302'")
    var car_df_T = SNA_ENTITY_CAR.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("car"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0302'")
    var case_df_T = SNA_ENTITY_CASE.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("case"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0302'")
    var tel_df_T = SNA_ENTITY_TEL.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("tel"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0302'")

    var df_T = person_df_T
      .union(car_df_T)
      .union(case_df_T)
      .union(tel_df_T).repartition(1)
    df_T.createTempView("df_T")
    //获取今天的实体数据
    //输入person_df_T_1，car_df_T_1，case_df_T_1，tel_df_T_1
    //输出df_T_1
    var person_df_T_1 = SNA_ENTITY_PERSON.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("person"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0303'")
    var car_df_T_1 = SNA_ENTITY_CAR.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("car"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0303'")
    var case_df_T_1 = SNA_ENTITY_CASE.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("case"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0303'")
    var tel_df_T_1 = SNA_ENTITY_TEL.getTable(ss)
      .select("MERGE_ID")
      .withColumn("label", lit("tel"))
      .withColumn("PROPERTIES", lit(""))
      .where("etl_upd_tm='0303'")
    var df_T_1 = person_df_T_1
      .union(car_df_T_1)
      .union(case_df_T_1)
      .union(tel_df_T_1).repartition(1)
    df_T_1.createTempView("df_T_1")


    //今天的增量实体
    //输入df_T，df_T_1
    //输出entity_increment
    var entity_increment = ss.sql(
      """
        |select t1.MERGE_ID,t1.label,t1.PROPERTIES
        |from df_T_1  t1
        |left join df_T  t2
        |on t1.MERGE_ID=t2.MERGE_ID
        |where t2.MERGE_ID is null
        |
      """.stripMargin)
    entity_increment.createTempView("entity_increment")
    //==========================================================================================================
    //获取昨天的关系
    //输入person_car_T,person_tel_T,tel_car_T,car_case_T
    //输出rel_T
    var person_car_T = SNA_REL_PERSON_CAR.getTable(ss).select("person_id", "car_id").where("etl_upd_tm='0302'")
    var person_tel_T = SNA_REL_PERSON_TEL.getTable(ss).select("person_id", "tel_id").where("etl_upd_tm='0302'")
    var tel_car_T = SNA_REL_TEL_CASE.getTable(ss).select("tel_id", "case_id").where("etl_upd_tm='0302'")
    var car_case_T = SNA_REL_CAR_CASE.getTable(ss).select("car_id", "case_id").where("etl_upd_tm='0302'")
    var rel_T = person_car_T
      .union(person_tel_T)
      .union(tel_car_T)
      .union(car_case_T).repartition(1)
    rel_T.createTempView("rel_T")


    //获取今天的关系
    //输入person_car_T_1,person_tel_T_1,tel_car_T_1,car_case_T_1
    //输出rel_T_1
    var person_car_T_1 = SNA_REL_PERSON_CAR.getTable(ss).select("person_id", "car_id").where("etl_upd_tm='0303'")
    var person_tel_T_1 = SNA_REL_PERSON_TEL.getTable(ss).select("person_id", "tel_id").where("etl_upd_tm='0303'")
    var tel_car_T_1 = SNA_REL_TEL_CASE.getTable(ss).select("tel_id", "case_id").where("etl_upd_tm='0303'")
    var car_case_T_1 = SNA_REL_CAR_CASE.getTable(ss).select("car_id", "case_id").where("etl_upd_tm='0303'")
    var rel_T_1 = person_car_T_1
      .union(person_tel_T_1)
      .union(tel_car_T_1)
      .union(car_case_T_1).repartition(1)
    rel_T_1.createTempView("rel_T_1")


    //获取今天的增量关系(格式：src_id,dest_id)
    //输入rel_T,rel_T_1
    //输出rel_increment
    var rel_increment = ss.sql(
      """
        |select
        |t1.person_id as srcID,
        |t1.car_id as destID
        |from rel_T_1 t1
        |left join rel_T t2
        |on t1.person_id=t2.person_id and t1.car_id=t2.car_id
        |where t2.person_id is null
      """.stripMargin)
    rel_increment.createTempView("rel_increment")


    //过滤那些srcid和destid均在今天实体增量的关系数据，输出新图集合
    //输入rel_increment，entity_increment
    //输出rel_T_1_T_1
    var rel_T_1_T_1 = ss.sql(
      """
        |select
        |   t1.srcID  as srcID,
        |   t1.destID  as  destID
        |from
        |   rel_increment t1
        |join
        |   entity_increment t2
        |on t1.srcID=t2.MERGE_ID
        |join
        |   entity_increment t3
        |on  t1.destID=t3.MERGE_ID
        |
        |
      """.stripMargin)
    rel_T_1_T_1.createTempView("rel_T_1_T_1")


    //根据上面的实体数据和关系数据创建新图并计算联通分量
    //输入rel_T_1_T_1，entity_increment
    //输出new_ccgraph
    var new_Graph_Vertexes = entity_increment.repartition(1).rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, None))
    var new_Graph_Edges = rel_T_1_T_1.repartition(1).rdd.map(row => Edge(row.getAs[String]("srcID").toLong, row.getAs[String]("destID").toLong, None))
    var new_graph = Graph(new_Graph_Vertexes, new_Graph_Edges)
    var new_ccgraph = new_graph.connectedComponents()


    //有已经分区过的图，进行二次划分
    //输入new_ccgraph
    //输出reRDD格式("VERTEX_ID", "CLUS_NO")
    var waitingsplit = new_ccgraph.vertices.map(e => (e._2, 1)).reduceByKey((x, y) => x + y).filter(e => e._2 > 1000)
    var waitingsplitArr = waitingsplit.map((_._1)).collect()
    var subgraph = new_ccgraph.filter(
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
      reRDD = new_ccgraph.vertices
    } else {
      reRDD = new_ccgraph.vertices.leftOuterJoin(subgraphV).map(e => (e._1, e._2._2.getOrElse(e._2._1)))
    }


    //得出今天所有划分好实体和社区reRDD，格式(vetextid,pid),补充详情信息
    //输入reRDD(T_1_vertextes_pid),entity_increment
    //输出full_V_T
    var T_1_vertextes_pid = reRDD.toDF("VERTEX_ID", "CLUS_NO")
    T_1_vertextes_pid.createTempView("T_1_vertextes_pid")
    var full_V_T = ss.sql(
      """
        |select
        |'' as key,
        |t1.CLUS_NO as CLUS_NO,
        |t1.VERTEX_ID as VERTEX_ID,
        |t2.label     as label,
        |'' as title,
        |t2.PROPERTIES  as PROPERTIES,
        |'' as etl_crt_tm,
        |'' as etl_upd_tm
        |from T_1_vertextes_pid t1
        |join entity_increment t2
        |on t2.MERGE_ID=t1.VERTEX_ID
      """.stripMargin)
    full_V_T.createTempView("full_V_T")


    //将补全后的今天的新图数据union昨天的数据
    //输入full_V_T，sna_cluster_vertext
    //输出T_T_1_cluster_vertext
    var sna_cluster_vertext = SNA_CLUSTER_VERTEX.getTable(ss)
    var T_T_1_cluster_vertext = sna_cluster_vertext.union(full_V_T)
    T_T_1_cluster_vertext.createTempView("T_T_1_cluster_vertext")


    //选出今天的关系增量中，关系两端的实体不全是今天的实体数据
    //输入rel_increment，rel_T_1_T_1
    //输出rel_T_T_1
    var rel_T_T_1 = ss.sql(
      """
        |select
        |t1.srcID,
        |t1.destID
        |from rel_increment t1
        |left join rel_T_1_T_1 t2
        |on t1.srcID=t2.srcID and t1.destID=t2.destID
        |where t2.srcID is null
      """.stripMargin)
    rel_T_T_1.createTempView("rel_T_T_1")



    //根据上一步获得的关系数据和实体数据，的出每个关系两端的实体所属分区，取两端的分区不同的那些关系
    //输入rel_T_T_1，T_T_1_cluster_vertext
    //输出t_merge_parts

    var t_merge_parts = ss.sql(
      """
        |select srcID,destID,srcCLUS_NO,destCLUS_NO from (
        |select
        |t1.srcID  as srcID,
        |t1.destID  as destID,
        |t2.CLUS_NO  as srcCLUS_NO,
        |t3.CLUS_NO  as destCLUS_NO
        |from rel_T_T_1 t1
        |join T_T_1_cluster_vertext t2
        |on t1.srcID=t2.VERTEX_ID
        |join T_T_1_cluster_vertext t3
        |on t1.destID=t3.VERTEX_ID
        |) temp where srcCLUS_NO<>destCLUS_NO
        |
      """.stripMargin)


    //根据上面得出的表，创建新图，并用联通分量来计算
    //输入t_merge_parts
    //输出ccgraph_aux

    var edges_aux = t_merge_parts.repartition(1).rdd.map(row => Edge(row.getAs[String]("srcCLUS_NO").toLong, row.getAs[String]("destCLUS_NO").toLong, None))
    var graph_aux = Graph.fromEdges(edges_aux, None)
    var ccgraph_aux: Graph[VertexId, None.type] = graph_aux.connectedComponents()

    //根据新划分的图得出那些社区要合并到一块
    //输入ccgraph_aux
    //输出new_cluster（"VERTEX_ID", "CLUS_NO"）
    ccgraph_aux.vertices.toDF("VERTEX_ID", "CLUS_NO").createTempView("new_cluster")


    //更新之前的实体节点表的实体所属分区
    //输入T_T_1_cluster_vertext,new_cluster
    //输出re_Vertext
    //得出今天的实体表
    var re_Vertext = ss.sql(
      """
        |select
        |t1.key,
        |case when t2.CLUS_NO is null then t1.CLUS_NO else t2.CLUS_NO end as CLUS_NO,
        |t1.VERTEX_ID as VERTEX_ID,
        |t1.label ,
        |t1.title,
        |t1.PROPERTIES,
        |t1.etl_crt_tm,
        |t1.etl_upd_tm
        |from
        |T_T_1_cluster_vertext t1
        |left join new_cluster t2
        |on t1.VERTEX_ID=t2.VERTEX_ID
        |
        |
      """.stripMargin)
    re_Vertext.createTempView("re_Vertext")


    //加工关系表
    //输入re_Vertext，rel_increment
    //输出re_Edges
    //得到今天的最终的关系数据re_Edges
    ss.sql(
      """
        |
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
        |from rel_increment t1
        |join re_Vertext t2
        |on t1.srcId=t2.VERTEX_ID
        |join re_Vertext t3
        |on t1.destID=t3.VERTEX_ID
      """.stripMargin).union(SNA_CLUSTER_EDGE.getTable(ss)).createTempView("re_Edges")

    println("=================")
    ss.sql(
      """
        |select * from re_Vertext
        |
      """.stripMargin).show()
    println("=================")
    ss.sql(
      """
        |select * from re_Edges
        |
      """.stripMargin).show()
    println("=================")




    //    T_T_1_cluster_vertext.show(100)
    //有边是的原先的表的社区id发生改变，下面re_Vertext_DF是更新后的表
    //    re_Vertext_DF.show(100)
    //    println("+++++++++++++++++++++++")


    //    val partColl = ccgraph_aux.vertices.map(e => (e._2, e._1)).groupByKey()
    //    println("ccgraph_aux.vertices", ccgraph_aux.vertices.partitions.length)
    //    sna_cluster_vertext.show(100)


    //需求1：输出需要合并的社区列表partColl
    //+-------+--------------+
    //|newPart|      oldParts|
    //+-------+--------------+
    //|  75464|[87322, 75464]|
    //|   1111|  [1111, 1343]|
    //|    865|    [865, 886]|
    //|      8|     [7654, 8]|
    //+-------+--------------+
    //    partColl.map(e => (e._1, e._2.toSeq.toArray)).toDF("newPart", "oldParts").show(100)

    //需求2：输出今天新增社区,输出(实体编号，社区id)
    //|increment_part|
    //+--------------+
    //|          3456|
    //|          1736|
    //|           431|
    //|          3748|
    //+--------------+
    //    val cc_Graph_Vertextes = ccgraph_aux.vertices.toDF("VERTEX_ID", "CLUS_NO")
    //    cc_Graph_Vertextes.createTempView("cc_Graph_Vertextes")
    //    ss.sql(
    //      """
    //        |select distinct(CLUS_NO) as increment_part from (
    //        |select t1.CLUS_NO
    //        |from T_1_vertextes_pid t1
    //        |left join cc_Graph_Vertextes t2
    //        |on t1.CLUS_NO=t2.CLUS_NO
    //        |where t2.CLUS_NO is null) temp
    //      """.stripMargin).show(100)
    //    personCountFilter()
    //    carCountFilter()
    //    telCountFilter()
    //    caseCountFilter()
    //    re_Vertext_DF.show(100)


  }
}
