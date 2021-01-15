package com.zm.tables.T5InitialGraph

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SNA_CLUSTER_EDGE {
  def getTable(ss: SparkSession) = {
    val inpudata: RDD[Row] = ss.sparkContext.parallelize(List(
      Row("", "8", "8", "inform", "", "800004", "tel", "134151", "tel", "", "", ""),
      Row("", "8", "8", "involved", "", "134151", "case", "243", "case", "", "", ""),
      Row("", "8", "8", "own", "", "985555", "person", "243", "person", "", "", ""),
      Row("", "20046", "20046", "involved", "", "78721", "case", "95879", "case", "", "", ""),
      Row("", "20046", "20046", "own", "", "20046", "person", "95879", "person", "", "", ""),
      Row("", "886", "886", "involved", "", "7666121", "case", "5641", "case", "", "", ""),
      Row("", "886", "886", "own", "", "20015", "person", "5641", "person", "", "", ""),
      Row("", "1343", "1343", "involved", "", "1313331", "case", "1343", "case", "", "", ""),
      Row("", "888", "888", "involved", "", "5411335", "case", "5665462", "case", "", "", ""),
      Row("", "888", "888", "own", "", "888", "person", "5665462", "person", "", "", ""),
      Row("", "8", "8", "involved", "", "88547", "case", "642", "case", "", "", ""),
      Row("", "8", "8", "own", "", "4516", "person", "642", "person", "", "", ""),
      Row("", "886", "886", "inform", "", "413334", "tel", "7242", "tel", "", "", ""),
      Row("", "20046", "20046", "own", "", "20046", "person", "412342141", "person", "", "", ""),
      Row("", "87322", "87322", "own", "", "87767", "person", "87322", "person", "", "", ""),
      Row("", "87322", "87322", "involved", "", "5141566", "case", "87322", "case", "", "", ""),
      Row("", "20046", "20046", "inform", "", "82611", "tel", "78721", "tel", "", "", ""),
      Row("", "87322", "87322", "own", "", "87767", "person", "208689", "person", "", "", ""),
      Row("", "7654", "7654", "own", "", "7654", "person", "10986", "person", "", "", ""),
      Row("", "888", "888", "own", "", "888", "person", "70595767", "person", "", "", ""),
      Row("", "1111", "1111", "own", "", "41231", "person", "958879", "person", "", "", ""),
      Row("", "1111", "1111", "involved", "", "55551", "case", "958879", "case", "", "", ""),
      Row("", "1111", "1111", "involved", "", "1111", "case", "958879", "case", "", "", ""),
      Row("", "8", "8", "own", "", "4516", "person", "90014", "person", "", "", ""),
      Row("", "1343", "1343", "involved", "", "1313331", "case", "98650", "case", "", "", ""),
      Row("", "7654", "7654", "own", "", "7654", "person", "1478890", "person", "", "", ""),
      Row("", "1111", "1111", "own", "", "41231", "person", "8191", "person", "", "", ""),
      Row("", "8", "8", "own", "", "83356", "person", "8", "person", "", "", ""),
      Row("", "8", "8", "involved", "", "88547", "case", "8", "case", "", "", ""),
      Row("", "87322", "87322", "own", "", "87767", "person", "87654", "person", "", "", ""),
      Row("", "87322", "87322", "involved", "", "5141566", "case", "87654", "case", "", "", ""),
      Row("", "886", "886", "involved", "", "7242", "case", "886", "case", "", "", ""),
      Row("", "886", "886", "involved", "", "7666121", "case", "886", "case", "", "", ""),
      Row("", "886", "886", "own", "", "20015", "person", "886", "person", "", "", ""),
      Row("", "886", "886", "own", "", "20015", "person", "413334", "person", "", "", ""),
      Row("", "20046", "20046", "own", "", "20046", "person", "82611", "person", "", "", ""),
      Row("", "886", "886", "inform", "", "413334", "tel", "7666121", "tel", "", "", ""),
      Row("", "8", "8", "own", "", "985555", "person", "800004", "person", "", "", ""),
      Row("", "888", "888", "inform", "", "70595767", "tel", "5411335", "tel", "", "", ""),
      Row("", "8", "8", "own", "", "83356", "person", "9056111", "person", "", "", ""),
      Row("", "9444", "9444", "own", "", "9444", "person", "70977211", "person", "", "", ""),
      Row("", "87322", "87322", "inform", "", "208689", "tel", "5141566", "tel", "", "", ""),
      Row("", "8", "8", "own", "", "985555", "person", "976", "person", "", "", ""),
      Row("", "8", "8", "involved", "", "88547", "case", "976", "case", "", "", ""),
      Row("", "1111", "1111", "inform", "", "8191", "tel", "1111", "tel", "", "", ""),
      Row("", "143543", "143543", "own", "", "143543", "person", "298701", "person", "", "", ""),
      Row("", "1343", "1343", "involved", "", "1313331", "case", "21416273", "case", "", "", "")
    ))
    var shema = StructType(List(
      StructField("KEY", StringType),
      //      StructField("CLUS_NO", StringType),
      StructField("SOURCE_CLUS_NO", StringType),
      StructField("TARGET_CLUS_NO", StringType),
      StructField("LABEL", StringType),
      StructField("TITLE", StringType),
      StructField("SOURCE_ID", StringType),
      StructField("SOURCE_LABEL", StringType),
      StructField("TARGET_ID", StringType),
      StructField("TARGET_LABEL", StringType),
      StructField("PROPERTIES", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inpudata, shema)
  }

}
