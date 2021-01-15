package com.zm2.tables.T1src

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object M6_AL_CF_RP_RELATEDPERSONNEL {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("个人", "身份证", "30001", "averson", "usa", "phili", "male", "29990101", "20300105", "20030102", "c", "250001", "2005", "30"),
      Row("公司", "身份证", "30001", "mcgrady", "usa", "houston", "male", "29990101", "20300105", "20030102", "c", "250001", "2005", "30"),
      Row("个人", "身份证", "30001", "pual", "canada", "phili", "male", "29990101", "20300105", "20030102", "c", "250001", "2005", "30"),
      Row("个人", "身份证", "30001", "jams", "ch", "phili", "male", "29990101", "20300105", "20030102", "c", "250001", "2005", "30"),
      Row("个人", "身份证", "30001", "allen", "usa", "phili", "male", "29990101", "20300105", "20030102", "c", "250001", "2005", "30"),
      Row("个人", "身份证", "30001", "west", "usa", "phili", "male", "29990101", "20300105", "20030102", "c", "250001", "2005", "30")
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("CUSTOMERTYPE", StringType),
      StructField("CERTIFICATETYPE", StringType),
      StructField("CERTIFICATENO", StringType),
      StructField("NAME", StringType),
      StructField("NATIONALITY", StringType),
      StructField("ADDRESSOFCERTIFICATE", StringType),
      StructField("SEX", StringType),
      StructField("CERTIFICATEOFLICENSE", StringType),
      StructField("CREATEDATE", StringType),
      StructField("UPDATEDATE", StringType),
      StructField("DRIVERTYPE", StringType),
      StructField("DRIVERLICENSENO", StringType),
      StructField("DRIVERLICENSEISSUEDATE", StringType),
      StructField("VALIDITYTERMOFLICENSE", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
