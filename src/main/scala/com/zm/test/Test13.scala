package com.zm.test

import org.apache.spark.sql.SparkSession

object Test13 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .config("spark.testing.memory", "2147480000")
      .appName("InitGraph")
      .master("local[2]")
      .getOrCreate()
    import ss.implicits._
    ss.sparkContext.parallelize(List(1, 2, 3, 4)).toDF("age").createTempView("tmpda")

    ss.table("tmpda").printSchema()

    ss.catalog.listTables().show()

    ss.sessionState.analyzer.batches.foreach(print(_))
  }

}
