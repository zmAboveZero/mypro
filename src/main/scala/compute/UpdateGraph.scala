package com.zm2.compute

import com.zm2.tables.T4entity.{SNA_ENTITY_CAR, SNA_ENTITY_CASE, SNA_ENTITY_PERSON, SNA_ENTITY_TEL}
import com.zm2.tables.T4rel.{SNA_REL_CAR_CASE, SNA_REL_PERSON_CAR, SNA_REL_PERSON_TEL, SNA_REL_TEL_CASE}
import com.zm2.tables.T5InitialGraph.SNA_CLUSTER_VERTEX
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.mutable.ArrayBuffer

object UpdateGraph {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      //      .config("spark.testing.memory", "2147480000")
      .appName("InitGraph")
      //      .master("local[2]")
      .getOrCreate()

    import ss.implicits._
    //获取今天的实体增量(格式：entity_id)
    //获取昨天的实体数据

    var person_df_T = SNA_ENTITY_PERSON.getTable(ss).select("MERGE_ID").where("etl_upd_tm='0302'")
    var car_df_T = SNA_ENTITY_CAR.getTable(ss).select("MERGE_ID").where("etl_upd_tm='0302'")
    var case_df_T = SNA_ENTITY_CASE.getTable(ss).select("MERGE_ID").where("etl_upd_tm='0302'")
    var tel_df_T = SNA_ENTITY_TEL.getTable(ss).select("MERGE_ID").where("etl_upd_tm='0302'")
    var df_T = person_df_T
      .union(car_df_T)
      .union(case_df_T)
      .union(tel_df_T).repartition(1)
    df_T.createTempView("df_T")
    //获取今天的实体数据

    //
    var person_df_T_1 = SNA_ENTITY_PERSON.getTable(ss).select("MERGE_ID").where("etl_upd_tm='0303'")
    var car_df_T_1 = SNA_ENTITY_CAR.getTable(ss).select("MERGE_ID").where("etl_upd_tm='0303'")
    var case_df_T_1 = SNA_ENTITY_CASE.getTable(ss).select("MERGE_ID").where("etl_upd_tm='0303'")
    var tel_df_T_1 = SNA_ENTITY_TEL.getTable(ss).select("MERGE_ID").where("etl_upd_tm='0303'")
    var df_T_1 = person_df_T_1
      .union(car_df_T_1)
      .union(case_df_T_1)
      .union(tel_df_T_1).repartition(1)
    df_T_1.createTempView("df_T_1")

    //今天的增量实体
    var entity_increment = ss.sql(
      """
        |select t1.MERGE_ID
        |from df_T_1  t1
        |left join df_T  t2
        |on t1.MERGE_ID=t2.MERGE_ID
        |where t2.MERGE_ID is null
        |
      """.stripMargin)

    entity_increment.createTempView("entity_increment")
    println("entity_increment", entity_increment.rdd.partitions.length)
    //==========================================================================================================
    //获取昨天的关系
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
    var rel_increment = ss.sql(
      """
        |
        |select
        |t1.person_id as srcID,
        |t1.car_id as destID
        |from rel_T_1 t1
        |left join rel_T t2
        |on t1.person_id=t2.person_id and t1.car_id=t2.car_id
        |where t2.person_id is null
      """.stripMargin)
    rel_increment.createTempView("rel_increment")

    println("rel_increment", rel_increment.rdd.partitions.length)
    //======================================================================================================
    // 获取昨天的已经分好的含有社区id的表(格式，entity_id,pid)
    var sna_cluster_vertext = SNA_CLUSTER_VERTEX.getTable(ss)
    sna_cluster_vertext.createTempView("SNA_CLUSTER_VERTEX")
    entity_increment
    rel_increment

    println("sna_cluster_vertext", sna_cluster_vertext.rdd.partitions.length)
    //=======================================================================================================================
    //关系的srcid和destid均在今天实体增量数据，输出新图集合
    //===================处理第一种情况，关系的srcid和destid均在今天实体增量数据，输出新图集合
    //将今天的增量实体广播出去，过滤今天的关系增量中，srcid和destid都在今天的实体增量中的关系，然后创建RDD[Edge]

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

    var new_Graph_Edges = rel_T_1_T_1.repartition(1).rdd.map(row => Edge(row.getAs[String]("srcID").toLong, row.getAs[String]("destID").toLong, None))
    var new_Graph_Vertexes = entity_increment.repartition(1).rdd.map(row => (row.getAs[String]("MERGE_ID").toLong, None))
    var new_graph = Graph(new_Graph_Vertexes, new_Graph_Edges)
    var new_ccgraph = new_graph.connectedComponents()

    var parts = new_ccgraph.vertices.map(e => e._2).distinct().collect()
    var vertextes_gt_1000 = ArrayBuffer[Graph[VertexId, None.type]]()
    var vertextes_lt_1000 = ArrayBuffer[Graph[VertexId, None.type]]()


    for (i <- parts) {
      val subgraph = new_ccgraph.filter(
        graph => graph,
        vpred = (vid: VertexId, part: VertexId) => part == i
      )
      if (subgraph.vertices.count() > 1000) {
        val partedGraph: Graph[VertexId, None.type] = LabelPropagation.run(subgraph, 100)
        vertextes_gt_1000 += (partedGraph)
      } else {
        vertextes_lt_1000 += (subgraph)
      }
    }
    var unionRdd: RDD[(VertexId, VertexId)] = null
    for (i <- vertextes_gt_1000 ++ vertextes_lt_1000) {
      if (unionRdd == null) {
        unionRdd = i.vertices
      } else {
        unionRdd = unionRdd.union(i.vertices)
      }
    }
    //得出今天所有划分好实体和社区，格式(vetextid,pid)
    var incremen_vertext_pid = unionRdd
    //    println(incremen_vertext_pid.partitions.length)
    var T_vertextes_pid = incremen_vertext_pid.toDF("VERTEX_ID", "CLUS_NO")
    println("T_vertextes_pid", T_vertextes_pid.rdd.partitions.length)
    T_vertextes_pid.createTempView("T_vertextes_pid")
    //===================================================================================================================
    //===================处理第二种情况，关系的srcid和destid均在不同社区，输出可以合并的社区集合
    //今天分好社区的实体df，union昨天的分好社区的SNA_CLUSTER_VERTEX表(转成RDD使用)得,然后创建schema和full_vertext_pid表
    //目的是计算那些分区会联合到一起，并得到集合格式(1,(1,2,4,5)),(7,(7,10,29))

    var T_T_1_cluster_vertext = sna_cluster_vertext.select("VERTEX_ID", "CLUS_NO").union(T_vertextes_pid)
    T_T_1_cluster_vertext.createTempView("T_T_1_cluster_vertext")
    //选出今天的关系增量中，那些连接今天的实体节点和昨天的实体节点，或者连接昨天不同的分区的实体节点的那些关系数据
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
    ss.sql(
      """
        |select VERTEX_ID, CLUS_NO from T_vertextes_pid
        |union all
        |select VERTEX_ID,CLUS_NO from  SNA_CLUSTER_VERTEX
        |
  """.stripMargin).createTempView("t_V_C")
    var t_merge_parts = ss.sql(
      """
        |select srcID,destID,srcCLUS_NO,destCLUS_NO from (
        |select
        |t1.srcID  as srcID,
        |t1.destID  as destID,
        |t2.CLUS_NO  as srcCLUS_NO,
        |t3.CLUS_NO  as destCLUS_NO
        |from rel_T_T_1 t1
        |join t_V_C t2
        |on t1.srcID=t2.VERTEX_ID
        |join t_V_C t3
        |on t1.destID=t3.VERTEX_ID
        |) temp where srcCLUS_NO<>destCLUS_NO
        |
      """.stripMargin)
    println("t_merge_parts", t_merge_parts.rdd.partitions.length)
    var edges_aux = t_merge_parts.repartition(1).rdd.map(row => Edge(row.getAs[String]("srcCLUS_NO").toLong, row.getAs[String]("destCLUS_NO").toLong, None))
    var graph_aux = Graph.fromEdges(edges_aux, None)

    println("graph_aux.vertices", graph_aux.vertices.partitions.length)
    var ccgraph_aux: Graph[VertexId, None.type] = graph_aux.connectedComponents()
    //===========================================================================================================================
    //===================处理第三种情况，关系的srcid和destid分别是昨天社区和今天社区，输出集合


    //T_T_1_cluster_vertext已经分好的昨天数据和今天实体增量数据

    //    var T_T_1_cluster_vertext_rdd = T_T_1_cluster_vertext.rdd.map(row => (row.getAs[String]("VERTEX_ID").toLong, row.getAs[String]("CLUS_NO").toLong))

    val re_DF = T_T_1_cluster_vertext
      .rdd.map(row => (row.getAs[String]("CLUS_NO").toLong, row.getAs[String]("VERTEX_ID").toLong))
      .leftOuterJoin(ccgraph_aux.vertices)
      //      .map(e => (e._1, e._2._2.getOrElse(e._2._1)))
      .map(e => (e._2._1, e._2._2.getOrElse(e._1)))
      .toDF("VERTEX_ID", "CLUS_NO")


    val partColl = ccgraph_aux.vertices.map(e => (e._2, e._1)).groupByKey()
    println("ccgraph_aux.vertices", ccgraph_aux.vertices.partitions.length)

    //昨天的节点数据
    //|key|  CLUS_NO|VERTEX_ID| label|title|PROPERTIES|etl_crt_tm|etl_upd_tm|
    //+---+---------+---------+------+-----+----------+----------+----------+
    //|   |     9444|     9444|person|     |          |          |          |
    //|   |        8|    83356|person|     |          |          |          |
    //|   |     1111|    41231|person|     |          |          |          |
    //|   |   143543|   143543|person|     |          |          |          |
    //|   |    87322|    87767|person|     |          |          |          |
    //|   |    20046|    20046|person|     |          |          |          |
    //|   |        8|   985555|person|     |          |          |          |
    //|   |      888|      888|person|     |          |          |          |
    //|   |        8|     4516|person|     |          |          |          |
    //|   |     7654|     7654|person|     |          |          |          |
    //|   |      886|    20015|person|     |          |          |          |
    //|   |        8|      243|   car|     |          |          |          |
    //|   |    20046|    95879|   car|     |          |          |          |
    //|   |      886|     5641|   car|     |          |          |          |
    //|   |     1343|     1343|   car|     |          |          |          |
    //|   |      888|  5665462|   car|     |          |          |          |
    //|   |        8|      642|   car|     |          |          |          |
    //|   |    87322|    87322|   car|     |          |          |          |
    //|   |     1111|   958879|   car|     |          |          |          |
    //|   |     1343|    98650|   car|     |          |          |          |
    //|   |        8|        8|   car|     |          |          |          |
    //|   |    87322|    87654|   car|     |          |          |          |
    //|   |      886|      886|   car|     |          |          |          |
    //|   |        8|      976|   car|     |          |          |          |
    //|   |     1343| 21416273|   car|     |          |          |          |
    //|   |        8|   134151|  case|     |          |          |          |
    //|   |      886|     7242|  case|     |          |          |          |
    //|   |    20046|    78721|  case|     |          |          |          |
    //|   |     1111|    55551|  case|     |          |          |          |
    //|   |     1343|  1313331|  case|     |          |          |          |
    //|   |      886|  7666121|  case|     |          |          |          |
    //|   |        8|    88547|  case|     |          |          |          |
    //|   |      888|  5411335|  case|     |          |          |          |
    //|   |    87322|  5141566|  case|     |          |          |          |
    //|   |     1111|     1111|  case|     |          |          |          |
    //|   |    20046|412342141|   tel|     |          |          |          |
    //|   |    87322|   208689|   tel|     |          |          |          |
    //|   |     7654|    10986|   tel|     |          |          |          |
    //|   |      888| 70595767|   tel|     |          |          |          |
    //|   |        8|    90014|   tel|     |          |          |          |
    //|   |     7654|  1478890|   tel|     |          |          |          |
    //|   |     1111|     8191|   tel|     |          |          |          |
    //|   |      886|   413334|   tel|     |          |          |          |
    //|   |    20046|    82611|   tel|     |          |          |          |
    //|   |        8|   800004|   tel|     |          |          |          |
    //|   |     9444| 70977211|   tel|     |          |          |          |
    //|   |   143543|   298701|   tel|     |          |          |          |
    //|   |905610111|905610111|   tel|     |          |          |          |
    //+---+---------+---------+------+-----+----------+----------+----------+
    sna_cluster_vertext.show(100)


    //需求1：输出需要合并的社区列表partColl
    //+-------+--------------+
    //|newPart|      oldParts|
    //+-------+--------------+
    //|  75464|[87322, 75464]|
    //|   1111|  [1111, 1343]|
    //|    865|    [865, 886]|
    //|      8|     [7654, 8]|
    //+-------+--------------+
    partColl.map(e => (e._1, e._2.toSeq.toArray)).toDF("newPart", "oldParts").show(100)

    //需求2：输出今天新增社区,输出(实体编号，社区id)
    //|increment_part|
    //+--------------+
    //|          3456|
    //|          1736|
    //|           431|
    //|          3748|
    //+--------------+
    val cc_Graph_Vertextes = ccgraph_aux.vertices.toDF("VERTEX_ID", "CLUS_NO")
    cc_Graph_Vertextes.createTempView("cc_Graph_Vertextes")
    ss.sql(
      """
        |select distinct(CLUS_NO) as increment_part from (
        |select t1.CLUS_NO
        |from T_vertextes_pid t1
        |left join cc_Graph_Vertextes t2
        |on t1.CLUS_NO=t2.CLUS_NO
        |where t2.CLUS_NO is null) temp
      """.stripMargin).show(100)



    //    personCountFilter()
    //    carCountFilter()
    //    telCountFilter()
    //    caseCountFilter()

    re_DF.show(100)


  }
}
