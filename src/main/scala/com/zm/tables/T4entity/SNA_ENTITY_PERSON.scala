package com.zm.tables.T4entity

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SNA_ENTITY_PERSON {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("", "20015", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "20046", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "41231", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "4516", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "83356", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "985555", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "888", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "87767", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "9444", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "143543", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),
      Row("", "7654", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0302"),




      Row("", "6736573", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "2565425", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "7625453", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "13435656", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "87865", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "445777", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "76543", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "2446788", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "998719", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303"),
      Row("", "9672", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0303")


    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("KEY", StringType),
      StructField("MERGE_ID", StringType),
      StructField("CUST_CODE", StringType),
      StructField("CUST_TYPE", StringType),
      StructField("CERT_TYPE", StringType),
      StructField("CERT_CODE", StringType),
      StructField("NAME", StringType),
      StructField("BIRTHDAY", StringType),
      StructField("ETHNIC", StringType),
      StructField("NATION", StringType),
      StructField("RESIDENCE", StringType),
      StructField("SEX", StringType),
      StructField("HEIGHT", StringType),
      StructField("DEAD_DATE", StringType),
      StructField("MARRIED_FLG", StringType),
      StructField("CERT_VALID_TERM", StringType),
      StructField("CERT_START_DATE", StringType),
      StructField("CERT_END_DATE", StringType),
      StructField("IS_VIP", StringType),
      StructField("SECURITY_LEVEL", StringType),
      StructField("CHECK_TRUE_FLG", StringType),
      StructField("CHECK_TRUE_DTTM", StringType),
      StructField("CHECK_TRUE_SYSCODE", StringType),
      StructField("CERT_CODE_CLR", StringType),
      StructField("CERT_CODE_FLG", StringType),
      StructField("NAME_CLR", StringType),
      StructField("NAME_FLG", StringType),
      StructField("src_sys", StringType),
      StructField("src_crt_tm", StringType),
      StructField("src_upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
