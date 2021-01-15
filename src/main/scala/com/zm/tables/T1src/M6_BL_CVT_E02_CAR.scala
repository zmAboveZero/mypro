package com.zm.tables.T1src

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object M6_BL_CVT_E02_CAR {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("c", "80230001", "depu", "20190306", "19780102"),
      Row("c", "80130657", "jonny", "20291201", "19970918"),
      Row("a", "80560879", "tizz", "20130316", "20200117"),
      Row("c", "80120055", "wuzz", "20090301", "19781208"),
      Row("a", "80670078", "rose", "20110607", "19901211")
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("DRIVERNOTYPE", StringType),
      StructField("DRIVERNO", StringType),
      StructField("DRIVERNAME", StringType),
      StructField("CREATEDATE", StringType),
      StructField("UPDATEDATE", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
