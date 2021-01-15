package com.zm.tables.T1src

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object M6_AL_CF_RP_PERSONLOSSINFO {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("身份证", "900001", "depu", "femal", "20190306", "19780102"),
      Row("身份证", "900657", "jonny", "male", "20291201", "19970918"),
      Row("身份证", "900879", "tizz", "femal", "20130316", "20200117"),
      Row("身份证", "900055", "wuzz", "male", "20090301", "19781208"),
      Row("身份证", "900078", "rose", "femal", "20110607", "19901211")
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("CERTIFICATETYPE", StringType),
      StructField("CERTIFICATENO", StringType),
      StructField("THEINJURYNAME", StringType),
      StructField("SEX", StringType),
      StructField("CREATEDATE", StringType),
      StructField("UPDATEDATE", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }


}
