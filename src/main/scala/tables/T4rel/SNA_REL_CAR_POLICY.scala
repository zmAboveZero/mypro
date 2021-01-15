package com.zm2.tables.T4rel

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SNA_REL_CAR_POLICY {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row()
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("key", StringType),
      StructField("policyno", StringType),
      StructField("car_id", StringType),
      StructField("rel_type", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
