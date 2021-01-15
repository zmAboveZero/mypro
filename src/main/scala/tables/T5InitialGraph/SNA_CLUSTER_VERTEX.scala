package com.zm2.tables.T5InitialGraph

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SNA_CLUSTER_VERTEX {
  def getTable(ss: SparkSession) = {
    val inpudata: RDD[Row] = ss.sparkContext.parallelize(List(
      Row("", "9444", "9444", "person", "", "", "", ""),
      Row("", "8", "83356", "person", "", "", "", ""),
      Row("", "1111", "41231", "person", "", "", "", ""),
      Row("", "143543", "143543", "person", "", "", "", ""),
      Row("", "87322", "87767", "person", "", "", "", ""),
      Row("", "20046", "20046", "person", "", "", "", ""),
      Row("", "8", "985555", "person", "", "", "", ""),
      Row("", "888", "888", "person", "", "", "", ""),
      Row("", "8", "4516", "person", "", "", "", ""),
      Row("", "7654", "7654", "person", "", "", "", ""),
      Row("", "886", "20015", "person", "", "", "", ""),
      Row("", "8", "243", "car", "", "", "", ""),
      Row("", "20046", "95879", "car", "", "", "", ""),
      Row("", "886", "5641", "car", "", "", "", ""),
      Row("", "1343", "1343", "car", "", "", "", ""),
      Row("", "888", "5665462", "car", "", "", "", ""),
      Row("", "8", "642", "car", "", "", "", ""),
      Row("", "87322", "87322", "car", "", "", "", ""),
      Row("", "1111", "958879", "car", "", "", "", ""),
      Row("", "1343", "98650", "car", "", "", "", ""),
      Row("", "8", "8", "car", "", "", "", ""),
      Row("", "87322", "87654", "car", "", "", "", ""),
      Row("", "886", "886", "car", "", "", "", ""),
      Row("", "8", "976", "car", "", "", "", ""),
      Row("", "1343", "21416273", "car", "", "", "", ""),
      Row("", "8", "134151", "case", "", "", "", ""),
      Row("", "886", "7242", "case", "", "", "", ""),
      Row("", "20046", "78721", "case", "", "", "", ""),
      Row("", "1111", "55551", "case", "", "", "", ""),
      Row("", "1343", "1313331", "case", "", "", "", ""),
      Row("", "886", "7666121", "case", "", "", "", ""),
      Row("", "8", "88547", "case", "", "", "", ""),
      Row("", "888", "5411335", "case", "", "", "", ""),
      Row("", "87322", "5141566", "case", "", "", "", ""),
      Row("", "1111", "1111", "case", "", "", "", ""),
      Row("", "20046", "412342141", "tel", "", "", "", ""),
      Row("", "87322", "208689", "tel", "", "", "", ""),
      Row("", "7654", "10986", "tel", "", "", "", ""),
      Row("", "888", "70595767", "tel", "", "", "", ""),
      Row("", "8", "90014", "tel", "", "", "", ""),
      Row("", "7654", "1478890", "tel", "", "", "", ""),
      Row("", "1111", "8191", "tel", "", "", "", ""),
      Row("", "886", "413334", "tel", "", "", "", ""),
      Row("", "20046", "82611", "tel", "", "", "", ""),
      Row("", "8", "800004", "tel", "", "", "", ""),
      Row("", "9444", "70977211", "tel", "", "", "", ""),
      Row("", "143543", "298701", "tel", "", "", "", ""),
      Row("", "905610111", "905610111", "tel", "", "", "", "")))
    var shema = StructType(List(
      StructField("key", StringType),
      StructField("CLUS_NO", StringType),
      StructField("VERTEX_ID", StringType),
      StructField("label", StringType),
      StructField("title", StringType),
      StructField("PROPERTIES", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inpudata, shema)
  }
}
