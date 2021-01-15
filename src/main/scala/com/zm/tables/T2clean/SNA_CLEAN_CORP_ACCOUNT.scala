package com.zm.tables.T2clean

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_CLEAN_CORP_ACCOUNT {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row()
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("cust_id", StringType),
      StructField("custtype", StringType),
      StructField("bank_acct", StringType),
      StructField("open_dt", StringType),
      StructField("bankcode", StringType),
      StructField("bank_name", StringType),
      StructField("status", StringType),
      StructField("acct_usage", StringType),
      StructField("curcode", StringType),
      StructField("bank_acct_name", StringType),
      StructField("acct_valid", StringType),
      StructField("bank_acct_clr", StringType),
      StructField("bank_acct_flag", StringType),
      StructField("src_sys", StringType),
      StructField("src_crt_tm", StringType),
      StructField("src_upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
