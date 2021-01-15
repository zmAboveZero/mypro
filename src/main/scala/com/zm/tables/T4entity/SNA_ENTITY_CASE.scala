package com.zm.tables.T4entity

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SNA_ENTITY_CASE {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("", "7666121", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "7242", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "78721", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "1111", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "55551", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "88547", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "134151", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "5411335", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "5141566", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "1313331", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),






      Row("", "565333", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "86540", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "7890", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "8468670", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "754350", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "265162650", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "562421", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "43254", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "725664", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "52161665", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "75464", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "556221", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303")
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("KEY", StringType),
      StructField("MERGE_ID", StringType),
      StructField("branchcode", StringType),
      StructField("notifyno", StringType),
      StructField("ctp_policyno", StringType),
      StructField("commer_policyno", StringType),
      StructField("acdnt_desc", StringType),
      StructField("acdnt_reason", StringType),
      StructField("acdnttype", StringType),
      StructField("major_acdntcode", StringType),
      StructField("loss_dt", StringType),
      StructField("acdntkind", StringType),
      StructField("loss_site", StringType),
      StructField("notify_insured_rel", StringType),
      StructField("notify_dt", StringType),
      StructField("notifytype", StringType),
      StructField("notify_tel", StringType),
      StructField("notify_addr", StringType),
      StructField("notify_name", StringType),
      StructField("notify_phone", StringType),
      StructField("receiver", StringType),
      StructField("input_dt", StringType),
      StructField("is_first_scene", StringType),
      StructField("is_other_comp_cust", StringType),
      StructField("crt_tm", StringType),
      StructField("upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
