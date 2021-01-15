package com.zm.tables.T4rel

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNA_REL_PERSON_CAR {
  def getTable(ss: SparkSession) = {
    var sc = ss.sparkContext
    var inpudata = List(
      Row("", "20015", "5641", "", "", "0302"),
      Row("", "20015", "886", "", "", "0302"),
      Row("", "20046", "95879", "", "", "0302"),
      Row("", "41231", "958879", "", "", "0302"),
      Row("", "4516", "642", "", "", "0302"),
      Row("", "83356", "8", "", "", "0302"),
      Row("", "985555", "976", "", "", "0302"),
      Row("", "985555", "243", "", "", "0302"),
      Row("", "888", "5665462", "", "", "0302"),
      Row("", "87767", "87322", "", "", "0302"),
      Row("", "87767", "87654", "", "", "0302"),


      Row("", "6736573", "865", "", "", "0303"),
      Row("", "6736573", "875", "", "", "0303"),
      Row("", "2565425", "9876", "", "", "0303"),
      Row("", "7625453", "3456", "", "", "0303"),
      Row("", "13435656", "1736", "", "", "0303"),
      Row("", "87865", "3748", "", "", "0303"),
      Row("", "87865", "94436", "", "", "0303"),
      Row("", "87865", "66666555", "", "", "0303"),
      Row("", "445777", "35555", "", "", "0303"),
      Row("", "76543", "948484", "", "", "0303"),
      Row("", "76543", "748494", "", "", "0303"),
      Row("", "76543", "193875", "", "", "0303"),
      Row("", "2446788", "94561", "", "", "0303"),
      Row("", "998719", "987560", "", "", "0303"),
      Row("", "9672", "431", "", "", "0303"),

      //连接昨天多个社区的关系
      Row("", "41231", "98650", "", "", "0303"),
      //连接今天和昨天不同分区的关系
      Row("", "6736573", "886", "", "", "0303")
      //测试数据，这条数据不应该选出，因为连接的是昨天同一个分区的实体
      //      Row("", "", "", "", "", "0303")

    )
    var inputRDD = sc.parallelize(inpudata)
    var schema = StructType(List(
      StructField("key", StringType),
      StructField("person_id", StringType),
      StructField("car_id", StringType),
      StructField("rel_type", StringType),
      StructField("etl_crt_tm", StringType),
      StructField("etl_upd_tm", StringType)
    ))
    ss.createDataFrame(inputRDD, schema)
  }


}
