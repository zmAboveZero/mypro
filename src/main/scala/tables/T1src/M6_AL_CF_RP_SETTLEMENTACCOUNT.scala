package com.zm.tables.T1src

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object M6_AL_CF_RP_SETTLEMENTACCOUNT {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("身份证", "900001", "depu", "20190306", "19780102"),
      Row("身份证", "900657", "jonny", "20291201", "19970918"),
      Row("身份证", "900879", "tizz", "20130316", "20200117"),
      Row("身份证", "900055", "wuzz", "20090301", "19781208"),
      Row("身份证", "900078", "rose", "20110607", "19901211")
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("CERTIFICATETYPE", StringType),
      StructField("CERTIFICATENO", StringType),
      StructField("PAYEEACCOUNTNAME", StringType),
      StructField("CREATEDATE", StringType),
      StructField("UPDATEDATE", StringType)
    ))

    ss.createDataFrame(inputRDD, schema)
  }

}
