package com.zm.test

import com.zm.tables.T3help.SNA_HELP_PERSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object Test3 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .config("spark.testing.memory", "2147480000")
      .appName("testhive")
      .master("local[2]")
      .getOrCreate()

    import ss.implicits._
    SNA_HELP_PERSON.getTable(ss).createTempView("tb1")
    ss.sparkContext.parallelize(
      List(
        (1, "tom", 12),
        (1, "jack", 66),
        (1, "tony", 76),
        (2, "robin", 36),
        (6, "jerry", 23),
        (5, "jessy", 75),
        (3, "robin", 16),
        (7, "mick", 87),
        (8, "jerry", 37)))
      .toDF("ID", "name", "age").createTempView("tb_info_1")


    ss.sparkContext.parallelize(List(
      (1, "tom", 12),
      (1, "jack", 66),
      (1, "tony", 76),
      (2, "robin", 36),
      (6, "jerry", 23),
      (5, "jessy", 75),
      (3, "robin", 16),
      (7, "mick", 87),
      (8, "jerry", 37)))
      .toDF("ID", "name", "age").createTempView("tb_info_2")


    ss.sql(
      """
        |explain
        |select a.id, count(1) as ct
        |from tb_info_1 a
        |join tb_info_2 b
        |on (a.id = b.id)
        |where a.age <> 75
        |group by a.id
      """.stripMargin).show(100)//.explain(true)


    //
    //
    //    ss.sql(
    //      """
    //        |select ID ,count(1) as ct from tb_info group by ID having ct<=3
    //      """.stripMargin).show()
    //
    //    ss.sql(
    //      """
    //        |select ID,row_number() over(partition by ID order by rand()) from tb_info where name<>'' limit 10
    //      """.stripMargin).explain(true)

    println(">>>>>>>>>>>>>>>>>>>>>>>")
    //    ss.sql(
    //      """
    //        |select ID,count(distinct ID) as ct  from tb_info where name<>'' group by ID having ct>8
    //      """.stripMargin).explain(true)

  }

}
