package com.zm.tables.T2clean

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_CLEAN_CORP_TELPHONE {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row()
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("cust_id", StringType),
      StructField("custtype", StringType),
      StructField("main_tel_flag", StringType),
      StructField("teltype", StringType),
      StructField("country", StringType),
      StructField("areacode", StringType),
      StructField("tel", StringType),
      StructField("tel_ext", StringType),
      StructField("tel_clr", StringType),
      StructField("tel_flag", StringType),
      StructField("src_sys", StringType),
      StructField("src_crt_tm", StringType),
      StructField("src_upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
