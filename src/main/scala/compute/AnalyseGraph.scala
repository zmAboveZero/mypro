package com.zm.compute

import com.zm.tables.T5InitialGraph.{SNA_CLUSTER_EDGE, SNA_CLUSTER_VERTEX}
import org.apache.spark.sql.SparkSession

object AnalyseGraph {

  def main(args: Array[String]): Unit = {
    var ss = SparkSession.builder()
      .config("spark.testing.memory", "2147480000")
      .appName("InitGraph")
      .master("local[2]")
      .getOrCreate()


    var SNA_CLUSTER_VERTEX_DF = SNA_CLUSTER_VERTEX.getTable(ss) //.where("etl_upd_tm='0302'")
    var SNA_CLUSTER_EDGE_DF = SNA_CLUSTER_EDGE.getTable(ss) //.where("etl_upd_tm='0302'")
    SNA_CLUSTER_VERTEX_DF.createTempView("SNA_CLUSTER_VERTEX")
    SNA_CLUSTER_EDGE_DF.createTempView("SNA_CLUSTER_EDGE")


    var VERTEX_rdd = SNA_CLUSTER_VERTEX_DF.rdd.map(row => (row.getAs[String]("VERTEX_ID").toLong, row.getAs[String]("CLUS_NO").toLong))
    var EDGE_rdd = SNA_CLUSTER_EDGE_DF.rdd.map(row => (row.getAs[Long]("SOURCE_ID"), row.getAs[Long]("TARGET_ID")))


    SNA_CLUSTER_VERTEX_DF.show()


    //每个社区节点数
    ss.sql(
      """
        |select CLUS_NO,count(VERTEX_ID)
        |from SNA_CLUSTER_VERTEX
        |group by CLUS_NO
      """.stripMargin)


    //每个社区车辆数
    ss.sql(
      """
        |select CLUS_NO,count(1)
        |from SNA_CLUSTER_VERTEX  t1
        |where t1.LABEL='car' group by CLUS_NO
      """.stripMargin)
    //每个社区人数
    ss.sql(
      """
        |select CLUS_NO,count(1)
        |from SNA_CLUSTER_VERTEX  t1
        |where t1.LABEL='person' group by CLUS_NO
      """.stripMargin)

    //每个社区电话数
    ss.sql(
      """
        |select CLUS_NO,count(1)
        |from SNA_CLUSTER_VERTEX  t1
        |where t1.LABEL='tel' group by CLUS_NO
      """.stripMargin)


    //边的所属社区，格式(SOURCE_ID,TARGET_ID,CLUS_NO)
    var rel_pid_df = ss.sql(
      """
        |select
        |   SOURCE_ID,
        |   TARGET_ID,
        |   CLUS_NO
        |from
        |(
        |   select
        |       t1.SOURCE_ID  as  SOURCE_ID,
        |       t1.TARGET_ID  as  TARGET_ID,
        |       t2.CLUS_NO    as  CLUS_NO
        |   from SNA_CLUSTER_EDGE t1
        |   join SNA_CLUSTER_VERTEX t2
        |   on t1.SOURCE_ID=t2.VERTEX_ID
        |   join SNA_CLUSTER_VERTEX t3
        |   on t1.TARGET_ID=t3.VERTEX_ID
        |) temp
        |where t2.CLUS_NO=t3.CLUS_NO
      """.stripMargin)


    ss.sql(
      """
        |
        |select CLUS_NO from SNA_CLUSTER_VERTEX  group by CLUS_NO having count(1) >100
        |
        |
        |
          """.stripMargin)


    //    每个社区中PR值最大的实体id
    //    ， 格式(CLUS_NO, vertextID)
    val rel_df_rdd = rel_pid_df.rdd.map(row => (row.getAs[Long]("CLUS_NO"), (row.getAs[Long]("SOURCE_ID"), row.getAs[Long]("TARGET_ID"))))
    var pids = rel_pid_df.select("CLUS_NO").distinct().rdd.map(row => row.getAs[Long]("CLUS_NO")).collect()


    //每个社区关系数
    ss.sql(
      """
        |select CLUS_NO ,count() from (
        |   select SOURCE_ID,TARGET_ID,t2.CLUS_NO ,t3.CLUS_NO
        |   from SNA_CLUSTER_EDGE t1
        |   join SNA_CLUSTER_VERTEX t2
        |   on t1.SOURCE_ID=t2.VERTEX_ID
        |   join SNA_CLUSTER_VERTEX t3
        |   on t1.TARGET_ID=t2.VERTEX_ID
        |   where t2.CLUS_NO=t3.CLUS_NO
        |) temp
        |group by t2.CLUS_NO
        |
          """.stripMargin)


    //更新分群基本信息表
    //， SNA_CLUSTER_BASE
    ss.sql(
      """
        |
        |insert overwrite  table  SNA_CLUSTER_BASE
        |
        |select * from XXX
        |
          """.stripMargin)

  }

}
