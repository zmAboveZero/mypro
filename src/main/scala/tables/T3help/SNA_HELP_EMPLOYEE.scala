package com.zm.tables.T3help

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_HELP_EMPLOYEE {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row()
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("KEY", StringType),
      StructField("merge_id", StringType),
      StructField("STATE", StringType),
      StructField("STARTDATE", StringType),
      StructField("ENDDATE", StringType),
      StructField("APPTIME", StringType),
      StructField("HELPCODE", StringType),
      StructField("BIRTHDAY", StringType),
      StructField("JOINDATE", StringType),
      StructField("STARTWORK", StringType),
      StructField("WORKDATE", StringType),
      StructField("COEFFICIENT", StringType),
      StructField("FUNDACCOUNT", StringType),
      StructField("ACADEMY", StringType),
      StructField("ADDRESS", StringType),
      StructField("APPARAT", StringType),
      StructField("CODE", StringType),
      StructField("CPICUID", StringType),
      StructField("EDUCATION", StringType),
      StructField("EMAIL", StringType),
      StructField("EMPNO", StringType),
      StructField("FAX", StringType),
      StructField("GENDER", StringType),
      StructField("HOMETELEPHONE", StringType),
      StructField("CARDTYPE", StringType),
      StructField("CARDNO", StringType),
      StructField("LINKAGECODE", StringType),
      StructField("MOBILEPHONE", StringType),
      StructField("NAME", StringType),
      StructField("ORG_ID", StringType),
      StructField("ORGSN", StringType),
      StructField("PASSWORD", StringType),
      StructField("POSITIONNAME", StringType),
      StructField("REMARK", StringType),
      StructField("SYSTEMCODE", StringType),
      StructField("TELEPHONE", StringType),
      StructField("TYPE", StringType),
      StructField("WORKADDRESS", StringType),
      StructField("ZIP", StringType),
      StructField("UNIT_CODE", StringType),
      StructField("BRANCH_CODE", StringType),
      StructField("certno_clr", StringType),
      StructField("certno_flag", StringType),
      StructField("src_sys", StringType),
      StructField("src_crt_tm", StringType),
      StructField("src_upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)


    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
