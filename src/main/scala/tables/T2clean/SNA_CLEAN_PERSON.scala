package com.zm.tables.T2clean

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SNA_CLEAN_PERSON {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("60000001", "团体", "身份证", "100003", "tom", "20100105", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("60003567", "团体", "身份证", "100025", "jerry", "20211119", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("60000290", "个人", "身份证", "100026", "tonny", "20010107", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("60000235", "团体", "身份证", "100689", "jack", "19781112", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("60013902", "个人", "身份证", "100741", "JZ", "19980105", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("60000899", "团体", "身份证", "100821", "jay", "20160103", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
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
      StructField("verify_TRUE_FLG", StringType),
      StructField("verify_TRUE_DTTM", StringType),
      StructField("verify_TRUE_SYSCODE", StringType),
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
