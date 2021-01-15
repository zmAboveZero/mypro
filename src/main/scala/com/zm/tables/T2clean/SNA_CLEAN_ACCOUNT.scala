package com.zm.tables.T2clean

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_CLEAN_ACCOUNT {
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
      StructField("BANK_ACCT", StringType),
      StructField("BANK_NAME", StringType),
      StructField("STATUS", StringType),
      StructField("CURCODE", StringType),
      StructField("BANK_ACCT_NAME", StringType),
      StructField("BANK_CODE", StringType),
      StructField("BANK_ACCT_CLR", StringType),
      StructField("BANK_ACCT_FLG", StringType),
      StructField("src_sys", StringType),
      StructField("src_crt_tm", StringType),
      StructField("src_upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)


    ))
    ss.createDataFrame(inputRDD, schema)
  }


}
