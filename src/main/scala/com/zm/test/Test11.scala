package com.zm.test

import com.zm.tables.T4entity.SNA_ENTITY_PERSON
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object Test11 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .config("spark.testing.memory", "2147480000")
      .appName("InitGraph")
      .master("local[2]")
      .getOrCreate()
//    println(SNA_CLUSTER_EDGE.getTable(ss).count())
    var person_df_T = SNA_ENTITY_PERSON.getTable(ss).select("MERGE_ID").where("etl_upd_tm='0302'")
    person_df_T.withColumn("label",lit("preson")).show()
  }

}
