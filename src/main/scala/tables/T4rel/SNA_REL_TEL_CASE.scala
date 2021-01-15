package com.zm2.tables.T4rel

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SNA_REL_TEL_CASE {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("", "413334", "7666121", "", "", "0302"),
      Row("", "413334", "7242", "", "", "0302"),
      Row("", "82611", "78721", "", "", "0302"),
      Row("", "8191", "1111", "", "", "0302"),
      Row("", "9056111", "88547", "", "", "0302"),
      Row("", "800004", "134151", "", "", "0302"),
      Row("", "70595767", "5411335", "", "", "0302"),
      Row("", "208689", "5141566", "", "", "0302"),



      Row("", "1345", "565333", "", "", "0303"),
      Row("", "1345", "7890", "", "", "0303"),
      Row("", "143566", "8468670", "", "", "0303"),
      Row("", "7611", "754350", "", "", "0303"),
      Row("", "7661999", "562421", "", "", "0303"),
      Row("", "7661999", "725664", "", "", "0303"),
      Row("", "78488111", "52161665", "", "", "0303"),
      Row("", "1796001", "556221", "", "", "0303")



    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("key", StringType),
      StructField("tel_id", StringType),
      StructField("case_id", StringType),
      StructField("rel_type", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
