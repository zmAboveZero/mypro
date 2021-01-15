package com.zm.tables.T1src

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object T_CRMS_D1_CUSTOMER {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext

    var inpudata = List(
      Row("600001", "个人", "身份证", "300055", "tom", "1980", "汉", "ch", "sh", "femal", "180", "29991231", "0", "2050", "1990", "2030", "0", "9", "1", "2012", "2013", "2023", "2090"),
      Row("600035", "个人", "身份证", "300024", "jerry", "1990", "苗", "ch", "yn", "femal", "169", "29991231", "0", "2050", "1990", "2030", "0", "9", "1", "2012", "2013", "2023", "2090"),
      Row("600567", "个人", "身份证", "300567", "linda", "2001", "土家", "ch", "sh", "male", "156", "29991231", "1", "2050", "1990", "2030", "0", "9", "1", "2012", "2013", "2023", "2090"),
      Row("600023", "个人", "身份证", "300056", "jack", "2020", "汉", "ch", "gz", "femal", "178", "29991231", "0", "2050", "1990", "2030", "0", "9", "1", "2012", "2013", "2023", "2090"),
      Row("600068", "个人", "身份证", "300032", "jordan", "2018", "回", "ch", "hb", "male", "185", "29991231", "1", "2050", "1990", "2030", "0", "9", "1", "2012", "2013", "2023", "2090"),
      Row("600009", "个人", "身份证", "300078", "wadw", "1970", "汉", "ch", "sh", "femal", "167", "29991231", "0", "2050", "1990", "2030", "0", "9", "1", "2012", "2013", "2023", "2090")
    )

    var inputRDD=sc.parallelize(inpudata)
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
      StructField("CHECK_TRUE_FLG", StringType),
      StructField("CHECK_TRUE_DTTM", StringType),
      StructField("CHECK_TRUE_SYSCODE", StringType),
      StructField("ETL_CRTDTTM", StringType),
      StructField("ETL_UPDDTTM", StringType)
    ))

    ss.createDataFrame(inputRDD, schema)
  }
}
