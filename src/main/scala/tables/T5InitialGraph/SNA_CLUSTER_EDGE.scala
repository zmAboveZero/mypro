package com.zm.tables.T5InitialGraph

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SNA_CLUSTER_EDGE {
  def getTable(ss: SparkSession) = {
    val inpudata: RDD[Row] = ss.sparkContext.parallelize(List(
      Row("", "", "", "", "", "", "", "", "", "", "")))
    var shema = StructType(List(
      StructField("KEY", StringType),
      StructField("CLUS_NO", StringType),
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
