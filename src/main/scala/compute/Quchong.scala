package com.zm2.compute

import com.zm2.tables.T3help.SNA_HELP_PERSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Quchong {
  def main(args: Array[String]): Unit = {
    var ss = SparkSession
      .builder()
      .config("spark.testing.memory", "2147480000")
      .appName("testhive")
      .master("local[2]")
      .getOrCreate()
    val strDF = SNA_HELP_PERSON.getTable(ss)
    val mergGroup: RDD[(String, Iterable[Row])] = strDF.rdd.groupBy(e => e.getAs[String]("MERGE_ID"))
    var newRow: RDD[Row] = mergGroup.map { row =>
      var crmsRow: Array[Row] = row._2.toArray.filter(indexNum => indexNum(27) == "CRMS")
      val dateRow: Array[Row] = row._2.toArray.filterNot(indexNum => indexNum(27) == "CRMS")
      var schemaRow: Row = null
      if (!crmsRow.isEmpty) {
        schemaRow = crmsRow(0)
      } else schemaRow = dateRow(0)


      var latestCrmsRow: Row = null
      if (crmsRow.isEmpty) {
        latestCrmsRow = new GenericRowWithSchema(
          Array("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
          schemaRow.schema)
      } else {
        latestCrmsRow = crmsRow(0)
        for (i <- crmsRow.indices) {
          if (crmsRow(i).getAs[String]("ETL_UPD_TM") > latestCrmsRow.getAs[String]("ETL_UPD_TM")) {
            latestCrmsRow = crmsRow(i)
          }
        }
      }
      var latestRow: Row = dateRow(0)
      for (i <- dateRow.indices) {
        if (dateRow(i).getAs[String]("ETL_UPD_TM") > latestRow.getAs[String]("ETL_UPD_TM")) {
          latestRow = dateRow(i)
        }
      }
      var crmsRowSEQ = latestCrmsRow.toSeq.toArray
      var latestRowSEQ = latestRow.toSeq.toArray

      var index = 0
      var coll = ArrayBuffer[Int]()
      while (index <= 31) {
        if ((crmsRowSEQ(index).equals("")) && (!latestRowSEQ(index).equals(""))) {
          crmsRowSEQ(index) = latestRowSEQ(index)
        }
        if (crmsRowSEQ(index).equals("")) {
          coll += index
        }
        index += 1
      }
      for (i <- coll) {
        var flag = true
        for (j <- row._2 if flag) {
          if (!j(i).equals("")) {
            crmsRowSEQ(i) = j(i)
            flag = false
          }
        }
      }
      Row.fromSeq(crmsRowSEQ)
    }
    newRow.collect().foreach(println(_))

    //    //5.创建RDD的shema
    //    val newRowSchema = strDF.schema
    //
    //    //6.创建返回值DataFrame
    //    val resultDF: DataFrame = ss.createDataFrame(newRow, newRowSchema)
    //
    //
    //    //8.使用saveAsTable将数据保存到hive中
    //    resultDF.write.mode(SaveMode.Append)
    //      .saveAsTable(s"${Database.SNA}.SNA_ENTITY_PERSON")


  }

}
