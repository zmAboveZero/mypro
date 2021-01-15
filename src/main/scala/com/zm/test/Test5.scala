package com.zm.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test5 {
  def main(args: Array[String]): Unit = {
    var ss = SparkSession.builder()
      .config("spark.testing.memory", "2147480000")
      .appName("InitGraph")
      .master("local[2]")
      .getOrCreate()
    var inputData = Array(1, 2, 3, 4)
    var count = Array(10, 20, 30, 40, 50, 60, 70, 80, 90, 100)
    var i = 0
    val inputRdd: RDD[Int] = ss.sparkContext.parallelize(inputData, 10)
    var aa = inputRdd.foreach(e => {
      count(e) = 999999
      i += 1
      println(i)
    }
    )
    inputRdd.collect()
    println(count.seq)
    //    inputRdd.map({ e =>
    //      i += 1
    //      count(e) = 9999
    //      e
    //    }
    //    )
    //    inputRdd.collect()
    //    println(i)
    //    println(count.seq)
  }

}
