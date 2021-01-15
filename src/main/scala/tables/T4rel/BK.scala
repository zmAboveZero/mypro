package com.zm.tables.T4rel

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object BK {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row()
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("", StringType),
      StructField("", StringType),
      StructField("", StringType),
      StructField("", StringType),
      StructField("", StringType),
      StructField("", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
