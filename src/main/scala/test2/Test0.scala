package test2

import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Test0 {
  def main(args: Array[String]): Unit = {

    //连接hive表
    var ss = SparkSession.builder()
      .appName("getHiveTable")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //获取今天的实体增量(格式：entity_id)
    var entity_increment = ss.sql(
      """select * from (select * from entity where dt="today") t1 left join (select * from entity where dt ="yestoday") t2
        | on t1.cust_no=t2.cust_no where t2.cust_no=null""".stripMargin)

    //获取今天的关系增量(格式：src_id,dest_id)
    var rel_increment = ss.sql(
      """select * from (select * from rel where dt="today") t1 left join (select * from rel where dt ="yestoday") t2
        ||on t1.srcid=t2.srcid and t1.destid=t2.destid where t2.srcid=null""".stripMargin)

    rel_increment.createTempView("rel_increment")


    //获取昨天的已经分好的含有社区id的表(格式，entity_id,pid)
    var SNA_CLUSTER_VERTEX = ss.sql(
      """
        |select * from SNA_CLUSTER_VERTEX where dt ='yestoday'
      """.stripMargin)

    SNA_CLUSTER_VERTEX.createTempView("SNA_CLUSTER_VERTEX")


    //=========================================处理第一种情况，关系的srcid和destid均在昨天同一社区情况，输出SOURCE_CLUS_NO<>TARGET_CLUS_NO的数据
    //关联今天的关系增量数据与昨天的含有pid的表也就是上一张表，获取src_id与dest_id的pid，并过滤掉SOURCE_CLUS_NO=TARGET_CLUS_NO的数据
    // 格式(SOURCE_ID,TARGET_ID,SOURCE_CLUS_NO,TARGET_CLUS_NO)
    //    var pid1_pid2 = ss.sql(
    //      """
    //        |select
    //        |	SOURCE_ID,
    //        |	TARGET_ID,
    //        |	SOURCE_CLUS_NO,
    //        |	TARGET_CLUS_NO
    //        |from
    //        |(
    //        |select
    //        |	t1.SOURCE_ID	as SOURCE_ID,
    //        |	t1.TARGET_ID	as TARGET_ID,
    //        |	t2.CLUS_NO		as SOURCE_CLUS_NO,
    //        |	t3.CLUS_NO		as TARGET_CLUS_NO
    //        |
    //        |from
    //        |	rel_increment  t1
    //        |join SNA_CLUSTER_VERTEX t2
    //        |on t1.SOURCE_ID=t2.VERTEX_ID
    //        |where t2.dt="yestoday"
    //        |
    //        |join SNA_CLUSTER_VERTEX t3
    //        |on t1.TARGET_ID=t3.VERTEX_ID
    //        |where t3.dt="yestoday"
    //        |) where SOURCE_CLUS_NO<>TARGET_CLUS_NO
    //        |
    //      """.stripMargin)
    //=========================================处理第二种情况，关系的srcid和destid均在今天实体增量数据，输出新图集合
    //将今天的增量实体广播出去，过滤今天的关系增量中，srcid和destid都在今天的实体增量中的关系，然后创建RDD[Edge]
    //    val set = entity_increment.select("CUST_CODE").rdd.map(row => row.get(1)).collect().toSet
    //    val value: RDD[Row] = rel_increment.select("SOURCE_ID", "TARGET_ID").rdd.filter(row => set.contains(row.get(1)) && set.contains(row.get(2)))
    //    val edges: RDD[Edge[None.type]] = value.map(row => Edge(row.get(1).asInstanceOf[VertexId], row.get(2).asInstanceOf[VertexId], None))

    var incre_rel = ss.sql(
      """
        |select * from rel_increment  t1
        |join entity_increment t2 on t1.SOURCE_ID=t2.VERTEX_ID
        |join entity_increment t3 on t1.TARGET_ID=t3.VERTEX_ID
        |
      """.stripMargin)

    var new_Graph_Edges = incre_rel.rdd.map(row => Edge(row.getAs("SOURCE_ID"), row.getAs("TARGET_ID"), None))

    //按照上一步的RDD[Edge]创建新Grap
    var new_graph = Graph.fromEdges(new_Graph_Edges, None)
    var new_ccgraph = new_graph.connectedComponents()

    var parts = new_ccgraph.vertices.map(e => e._2).distinct().collect()
    var vertextes_gt_1000 = ArrayBuffer[Graph[VertexId, None.type]]()
    var vertextes_lt_1000 = ArrayBuffer[Graph[VertexId, None.type]]()

    for (part <- parts) {
      val subgraph = new_ccgraph.filter(
        graph => graph,
        vpred = (vid: VertexId, part: VertexId) => part == part
      )
      if (subgraph.vertices.count() > 1000) {
        val partedGraph: Graph[VertexId, None.type] = LabelPropagation.run(subgraph, 100)
        vertextes_gt_1000 += (partedGraph)
      } else {
        vertextes_lt_1000 += (subgraph)
      }
    }


    var temp: RDD[(VertexId, VertexId)] = null
    vertextes_gt_1000
    vertextes_lt_1000
    for (i <- vertextes_gt_1000 ++ vertextes_lt_1000) {
      temp = temp.union(i.vertices)
    }

    //今天新增的分区,union昨天的分区，格式(vetextid,pid)
    var incremen_vertext_pid = temp

    temp = temp.union(SNA_CLUSTER_VERTEX.rdd.map(row => (row.getAs("VERTEX_ID"), row.getAs("CLUS_NO"))))
    var temp2 = temp.map(e => Row(e._1, e._2))
    //===================================================处理第三种情况，关系的srcid和destid均在昨天不同社区，输出可以合并的社区集合


    var schema = StructType(List(
      StructField("VERTEX_ID", LongType),
      StructField("CLUS_NO", LongType)
    ))
    var full_vertext_DF = ss.createDataFrame(temp2, schema)
    full_vertext_DF.createTempView("full_vertext_pid")
    var union_rel = ss.sql(
      """
        |select
        |	SOURCE_ID,
        |	TARGET_ID,
        |	SOURCE_CLUS_NO,
        |	TARGET_CLUS_NO
        |from
        |(
        |select
        |	t1.SOURCE_ID	as SOURCE_ID,
        |	t1.TARGET_ID	as TARGET_ID,
        |	t2.CLUS_NO		as SOURCE_CLUS_NO,
        |	t3.CLUS_NO		as TARGET_CLUS_NO
        |from
        |	rel_increment  t1
        |join full_vertext_pid t2
        |on t1.SOURCE_ID=t2.VERTEX_ID
        |join full_vertext_pid t3
        |on t1.TARGET_ID=t3.VERTEX_ID
        |) where SOURCE_CLUS_NO<>TARGET_CLUS_NO
        |
  """.stripMargin)
    var union_rel_rdd = union_rel.rdd.map(row => Edge(row.getAs[Long]("SOURCE_ID"), row.getAs[Long]("TARGET_ID"),None))
    var temp_graph = Graph.fromEdges(union_rel_rdd, None)
    var temp_ccgraph = temp_graph.connectedComponents()
    //===================================================处理第四种情况，关系的srcid和destid分别是昨天社区和今天社区，输出集合
    //输出(1,(2,3,4,1)),(5,(12,34,5,121))
    val relatedcommuiy = temp_ccgraph.vertices.map(e => (e._2, e._1)).groupByKey()
    val re: Array[(VertexId, Iterable[VertexId])] = relatedcommuiy.collect()

    //输出(1,1)(2,1)(3,1)(5,1)(7,1)(9,1)
    val increment_part: RDD[(VertexId, VertexId)] = incremen_vertext_pid.filter { e =>
      for (i <- re) {
        if (i._2.toArray.contains(e)) {
          return false
        }
      }
      true
    }
    increment_part.collect()



    //需求1：输出今天新增社区
    //需求2：输出需要合并的社区列表


    //    var setarray = ArrayBuffer[Set[Long]]()
    //    src_dest_pid_pid.rdd.map { row =>
    //      var pid1 = row.getAs("pid1")
    //      var pid2 = row.getAs("pid2")
    //      for (set <- setarray) {
    //        if (set.contains(pid1) || set.contains(pid2)) {
    //          set.+(pid1) + (pid2)
    //        } else {
    //          setarray += Set(pid1, pid2)
    //        }
    //      }
    //    }


    //    src_dest_pid_pid.where("pid")


    //加载昨天的节点数据和关系数据创建图
    val t_entity_DF: DataFrame = ss.table("entity").where("dt=yestoday")
    val t_rel_DF: DataFrame = ss.table("rel").where("dt=yestoday")



    //    val vertexes: RDD[(Long, Map[String, Nothing])] = t_entity_DF.rdd.map(row => (row.getLong(1), Map("subGraphID" -> row.getAs("subGraphID"))))
    //    val rel: RDD[Edge[Map[String, Nothing]]] = t_rel_DF.rdd.map(row => Edge(row.getAs("srcid"), row.getAs("destId"), Map("prop" -> row.getAs("prop"))))
    //    var graph = Graph(vertexes, rel)


    var today_entity_DF = ss.table("entity").where("dt=today")
    var today_tel_DF = ss.table("rel").where("dt=today")


    var today = today_entity_DF.rdd.map(row => (row.getAs("cust_no"), row))

    //    var taday_rel = today_tel_DF.rdd.map(row => (((row.getAs("srcId"), row.getAs("destId"))), row))
    //    taday_rel.leftOuterJoin(today_tel_DF)

    //加载今天的节点数据和关系数据创建图
    //计算增量数据(关系，实体)


  }
}
