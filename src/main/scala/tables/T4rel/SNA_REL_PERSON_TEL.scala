package com.zm2.tables.T4rel

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SNA_REL_PERSON_TEL {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("", "20015", "413334", "", "", "0302"),
      Row("", "20046", "412342141", "", "", "0302"),
      Row("", "20046", "82611", "", "", "0302"),
      Row("", "41231", "8191", "", "", "0302"),
      Row("", "4516", "90014", "", "", "0302"),
      Row("", "83356", "9056111", "", "", "0302"),
      Row("", "985555", "800004", "", "", "0302"),
      Row("", "888", "70595767", "", "", "0302"),
      Row("", "87767", "208689", "", "", "0302"),
      Row("", "9444", "70977211", "", "", "0302"),
      Row("", "143543", "298701", "", "", "0302"),
      Row("", "7654", "10986", "", "", "0302"),
      Row("", "7654", "1478890", "", "", "0302"),


      Row("", "6736573", "1345", "", "", "0303"),
      Row("", "2565425", "6115", "", "", "0303"),
      Row("", "7625453", "143566", "", "", "0303"),
      Row("", "13435656", "7611", "", "", "0303"),
      Row("", "87865", "7661999", "", "", "0303"),
      Row("", "445777", "78471910", "", "", "0303"),
      Row("", "76543", "78488111", "", "", "0303"),
      Row("", "2446788", "890171", "", "", "0303"),
      Row("", "998719", "1435551", "", "", "0303"),
      Row("", "9672", "1796001", "", "", "0303"),


      //昨天连接不同分区的关系
      Row("", "4516", "10986", "", "", "0303"),

      //今天和昨天连接不同分区的关系
      Row("", "998719", "208689", "", "", "0303")
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("key", StringType),
      StructField("person_id", StringType),
      StructField("tel_id", StringType),
      StructField("rel_type", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
