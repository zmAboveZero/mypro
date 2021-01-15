package com.zm.tables.T2clean

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_CLEAN_CORP {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row()
    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("cust_id", StringType),
      StructField("org_code", StringType),
      StructField("name", StringType),
      StructField("uscc_code", StringType),
      StructField("name_en", StringType),
      StructField("abbr", StringType),
      StructField("abbr_en", StringType),
      StructField("lealper_name", StringType),
      StructField("certtype", StringType),
      StructField("cert_no", StringType),
      StructField("biz_stat", StringType),
      StructField("rgst_addr", StringType),
      StructField("uscc_code_clr", StringType),
      StructField("org_code_clr", StringType),
      StructField("name_clr", StringType),
      StructField("uscc_code_flg", StringType),
      StructField("org_code_flg", StringType),
      StructField("name_flg", StringType),
      StructField("src_sys", StringType),
      StructField("src_crt_tm", StringType),
      StructField("src_upd_tm", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)


    ))
    ss.createDataFrame(inputRDD, schema)
  }

}
