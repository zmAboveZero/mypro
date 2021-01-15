package com.zm.test

import com.zm.tables.T1src.M6_AL_CF_RP_PERSONLOSSINFO
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Test12 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .config("spark.testing.memory", "2147480000")
      .appName("InitGraph")
      .master("local[2]")
      .getOrCreate()
    import ss.implicits._
    val temp_table = M6_AL_CF_RP_PERSONLOSSINFO.getTable(ss)
    var newSchema = temp_table.schema.add(StructField("mergId", StringType))
    val newRows = temp_table.rdd.zipWithUniqueId().map(ele => Row.fromSeq(ele._1.toSeq.:+(ele._2.toString).asInstanceOf[Seq[String]]))
    newRows.foreach(println(_))
    val newTable = ss.createDataFrame(newRows, newSchema)
    //    newRows.toDF().show()
    //    ss.sparkContext.parallelize(List((1, 2))).toDF()
    newTable.createTempView("temp")
    newTable.printSchema()
    ss.sql(
      """
        |select CERTIFICATETYPE ,CERTIFICATENO,CREATEDATE,UPDATEDATE,mergId,row_number() over(PARTITION BY CREATEDATE AND UPDATEDATE ORDER BY mergId) as rn  from temp
      """.stripMargin).show()


  }

}
