package com.zm2.tables.T3help

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SNA_HELP_PERSON {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      //      Row("abc", "10001", "", "", "", "", "tom", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "CRMS", "", "", "", "20100101"),
      //      Row("abc", "10001", "", "", "", "", "tom", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "CRMS", "", "", "", "20200101"),
      //      Row("abc", "10001", "", "", "", "", "tom", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "CRMS", "", "", "", "20300101"),
      Row("", "10001", "", "", "", "", "jerry", "1980", "han", "ch", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "M6", "", "", "", "20200106"),
      Row("", "10001", "", "", "", "", "linda", "2000", "han", "usa", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "M6", "", "", "", "20200103"),
      Row("", "10001", "", "", "", "", "jack", "1975", "han", "un", "", "", "", "", "placehold", "", "", "", "", "", "", "", "", "", "", "", "", "M6", "", "", "", "20200105"),
      //      Row("abc", "10002", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "CRMS", "", "", "", "20200101"),
      Row("", "10002", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "20200101")
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
      StructField("ETL_UPD_TM", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
