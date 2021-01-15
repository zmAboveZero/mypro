package com.zm.tables.T4entity

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_ENTITY_REPAIR {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row()
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("KEY", StringType),
      StructField("repair_fac_code", StringType),
      StructField("repair_fac_name", StringType),
      StructField("branch_code", StringType),
      StructField("repairr_no", StringType),
      StructField("auto_code", StringType),
      StructField("repair_fac_addr", StringType),
      StructField("repair_fac_stat", StringType),
      StructField("contactor", StringType),
      StructField("contactor_tel", StringType),
      StructField("is_default", StringType),
      StructField("crt_tm", StringType),
      StructField("upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
