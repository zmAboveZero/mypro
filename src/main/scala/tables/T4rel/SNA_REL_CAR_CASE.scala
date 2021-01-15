package com.zm2.tables.T4rel

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SNA_REL_CAR_CASE {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("", "5641", "7666121", "", "", "0302"),
      Row("", "886", "7242", "", "", "0302"),
      Row("", "886", "7666121", "", "", "0302"),
      Row("", "95879", "78721", "", "", "0302"),
      Row("", "958879", "1111", "", "", "0302"),
      Row("", "958879", "55551", "", "", "0302"),
      Row("", "642", "88547", "", "", "0302"),
      Row("", "8", "88547", "", "", "0302"),
      Row("", "976", "88547", "", "", "0302"),
      Row("", "243", "134151", "", "", "0302"),
      Row("", "5665462", "5411335", "", "", "0302"),
      Row("", "87322", "5141566", "", "", "0302"),
      Row("", "87654", "5141566", "", "", "0302"),
      Row("", "1343", "1313331", "", "", "0302"),
      Row("", "98650", "1313331", "", "", "0302"),
      Row("", "21416273", "1313331", "", "", "0302"),


      Row("", "865", "565333", "", "", "0303"),
      Row("", "865", "86540", "", "", "0303"),
      Row("", "875", "7890", "", "", "0303"),
      Row("", "9876", "7890", "", "", "0303"),
      Row("", "3456", "8468670", "", "", "0303"),
      Row("", "1736", "754350", "", "", "0303"),
      Row("", "3748", "265162650", "", "", "0303"),
      Row("", "94436", "562421", "", "", "0303"),
      Row("", "94436", "43254", "", "", "0303"),
      Row("", "66666555", "725664", "", "", "0303"),
      Row("", "35555", "725664", "", "", "0303"),
      Row("", "948484", "725664", "", "", "0303"),
      Row("", "748494", "52161665", "", "", "0303"),
      Row("", "193875", "52161665", "", "", "0303"),
      Row("", "94561", "75464", "", "", "0303"),
      Row("", "987560", "75464", "", "", "0303"),
      Row("", "431", "556221", "", "", "0303")


    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("key", StringType),
      StructField("car_id", StringType),
      StructField("case_id", StringType),
      StructField("rel_type", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
