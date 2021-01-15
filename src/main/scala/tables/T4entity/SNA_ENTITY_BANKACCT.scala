package com.zm2.tables.T4entity

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_ENTITY_BANKACCT {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row()
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("KEY", StringType),
      StructField("ACCT_ID", StringType),
      StructField("BANK_ACCT", StringType),
      StructField("BANK_NAME", StringType),
      StructField("STATUS", StringType),
      StructField("CURCODE", StringType),
      StructField("BANK_ACCT_NAME", StringType),
      StructField("BANK_CODE", StringType),
      StructField("crt_tm", StringType),
      StructField("upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
