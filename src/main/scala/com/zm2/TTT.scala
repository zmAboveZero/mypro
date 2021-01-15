package com.zm2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object TTT {


  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[2]").appName("HiveConnApp")
//      .enableHiveSupport()
      .getOrCreate()
    var strDF = ss.table("sparkhive.T_CRMS_D1_CUSTOMER")
    val mergGroup: RDD[(Int, Iterable[Row])] = strDF.rdd.groupBy(e => e.getAs("MERGE_ID"))
    var re = mergGroup.map { row =>
      val crmsRow: Row = row._2.toArray.filter(indexNum => indexNum(28) == "CRMS")(0)
      val dateRow: Array[Row] = row._2.toArray.filterNot(indexNum => indexNum(28) == "CRMS")
      var earlistRow = dateRow(0)
      for (i <- dateRow.indices) {
        if (dateRow(i).getAs("etlDate").asInstanceOf[Int] < earlistRow.getAs("etlDate").asInstanceOf[Int]) {
          earlistRow = dateRow(i)
        }
      }
      var crmsRowSEQ = crmsRow.toSeq.toArray
      var earlistRowSEQ = earlistRow.toSeq.toArray
      var index = 0
      var coll = ArrayBuffer[Int]()
      while (index <= 32) {
        if ((crmsRowSEQ(index) == null) && (earlistRowSEQ(index) != null)) {
          crmsRowSEQ(index) = earlistRowSEQ(index)
        }
        if (crmsRowSEQ(index) == null) {
          coll += index
        }
        index += 1
      }
      for (i <- coll) {
        var flag = true
        for (j <- row._2 if flag) {
          if (j(i) != null) {
            crmsRowSEQ(i) = j(i)
            flag = false
          }
        }
      }
      Row(crmsRowSEQ)
    }
    println(re.collect())

    //（1）、使用saveAsTable，如果表不存在就创建，如果存在就追加数据

    //    strDF.write
    //      .mode(SaveMode.Append)
    //      .partitionBy("xxxx")
    //      .saveAsTable("xxxxxx")

    //4、程序运行结束，关闭资源
    //    spark.stop()
  }

}
