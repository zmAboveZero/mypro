package com.zm.tables.T2clean

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_CLEAN_TELPHONE {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row()
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("CUST_CODE", StringType),
      StructField("CUST_NAME", StringType),
      StructField("CERT_TYPE", StringType),
      StructField("CERT_CODE", StringType),
      StructField("TELTYPE", StringType),
      StructField("COUNTRYCODE", StringType),
      StructField("AREACODE", StringType),
      StructField("TEL_EXT", StringType),
      StructField("FULL_TEL", StringType),
      StructField("FULL_TEL_CLR", StringType),
      StructField("FULL_TEL_FLG", StringType),
      StructField("src_sys", StringType),
      StructField("src_crt_tm", StringType),
      StructField("src_upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)

    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
