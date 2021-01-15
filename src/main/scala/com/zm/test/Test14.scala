package com.zm.test

import org.apache.spark.sql.SparkSession

object Test14 {
  def main(args: Array[String]): Unit = {

    println(System.currentTimeMillis() / 10)

//    var ss = SparkSession
//      .builder()
//      .config("spark.testing.memory", "2147480000")
//      .appName("testhive")
//      .master("local[2]")
//      .getOrCreate()
//
//    //    ss.udf.register("hello", hello())
//    def hello(): String = {
//      return "fsfsa"
//    }
//
//    ss.udf.register("hello", hello _)
//    ss.sql(
//      """
//        |select hello()
//      """.stripMargin).show()
  }


}
