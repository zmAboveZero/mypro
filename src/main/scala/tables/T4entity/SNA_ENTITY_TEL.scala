package com.zm2.tables.T4entity

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_ENTITY_TEL {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("", "413334", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "412342141", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "82611", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "8191", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "90014", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "905610111", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "800004", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "70595767", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "208689", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "70977211", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "298701", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "10986", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "1478890", "", "", "", "", "", "", "", "", "", "", "", "0302"),




      Row("", "1345", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "6115", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "143566", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "7611", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "7661999", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "78471910", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "78488111", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "890171", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "1435551", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "1796001", "", "", "", "", "", "", "", "", "", "", "", "0303")
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("KEY", StringType),
      StructField("MERGE_ID", StringType),
      StructField("TELTYPE", StringType),
      StructField("COUNTRYCODE", StringType),
      StructField("AREACODE", StringType),
      StructField("TEL_EXT", StringType),
      StructField("FULL_TEL", StringType),
      StructField("FULL_TEL_CLR", StringType),
      StructField("FULL_TEL_FLG", StringType),
      StructField("src_sys", StringType),
      StructField("src_crt_tm", StringType),
      StructField("src_upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)

    ))
    ss.createDataFrame(inputRDD, schema)
  }


}
