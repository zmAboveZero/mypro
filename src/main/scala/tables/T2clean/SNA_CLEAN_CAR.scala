package com.zm2.tables.T2clean

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_CLEAN_CAR {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row()
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("VIN", StringType),
      StructField("PLATE_NO", StringType),
      StructField("PLATE_COLOR", StringType),
      StructField("PLATE_TYPE", StringType),
      StructField("ENGINE_NO", StringType),
      StructField("BRAND_CODE", StringType),
      StructField("BRAND_NAME", StringType),
      StructField("FACTORY_CODE", StringType),
      StructField("FACTORY_NAME", StringType),
      StructField("MOLD_CODE", StringType),
      StructField("MOLD_NAME", StringType),
      StructField("PRODUCING_AREA", StringType),
      StructField("VEHICLE_GROUP_CODE", StringType),
      StructField("VEHICLE_GROUP_NAME", StringType),
      StructField("VEHICLE_SERIES_CODE", StringType),
      StructField("VEHICLE_SERIES_NAME", StringType),
      StructField("VEHICLE_VARIETY_CODE", StringType),
      StructField("VEHICLE_VARIETY_NAME", StringType),
      StructField("BRANCH_CODE", StringType),
      StructField("JINGYOU_VEHICLE_VARIETY_CODE", StringType),
      StructField("JINGYOU_VEHICLE_VARIETY_NAME", StringType),
      StructField("VEHICLE_USE_CHARACTER", StringType),
      StructField("COTY", StringType),
      StructField("PURCHASE_PRICE", StringType),
      StructField("VEHICLE_ACTUAL_PRICE", StringType),
      StructField("VEHICLE_REGISTER_DATE", StringType),
      StructField("OWNER_CERT_TYPE", StringType),
      StructField("OWNER_CERT_CODE", StringType),
      StructField("OWNER_NAME", StringType),
      StructField("IS_SELFDEFINED_MOLD", StringType),
      StructField("vin_clr", StringType),
      StructField("vin_flag", StringType),
      StructField("plate_no", StringType),
      StructField("plate_no_flag", StringType),
      StructField("src_sys", StringType),
      StructField("src_crt_tm", StringType),
      StructField("src_upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)

    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
