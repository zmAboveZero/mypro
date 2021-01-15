package com.zm.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Test2 {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      //      .config("spark.testing.memory", "2147480000")
      .appName("testhive")
      //      .master("local[2]")
      .getOrCreate()
    import ss.implicits._
    //    val ds: Dataset[Int] = List(1, 2, 3, 4).toDS()
    //    val df: DataFrame = List(1, 2, 3, 4).toDF()
    var persons: RDD[Person] = ss.sparkContext.parallelize[Person](List[Person](Person("tom", 2), Person("jerry", 8)))
    //    var persons = List(Person("tom", 2), Person("jerry", 8))
    persons.toDF().show()
    //    persons.foreach(println(_))
    //    persons.show()


    //    import ss.implicits._
    //    var row = List(Row(1), Row(2), Row(3), Row(4), Row(5))
    //    var row2 = List(Array(1, 5), Array(2, 5), Array(3, 5), Array(4, 5), Array(5, 5))
    //
    //    val rdd: RDD[Row] = ss.sparkContext.parallelize(row)
    //    var schema = StructType(List(
    //      StructField("age", IntegerType)
    //    ))
    //    var t1 = ss.createDataFrame(rdd, schema)
    //
    //    println(t1.schema)


    //    val rdd2: RDD[Array[Int]] = ss.sparkContext.parallelize(row2)
    //
    //    row2.toDF("1").foreach(e => println(e.getAs("1")))

    //    case class  Person(age:Int)
    //
    //  var w=ss.sparkContext.parallelize(List(Person(1), Person(2), Person(3), Person(4), Person(5)))
    ////    w.toDS()
    //    val hello: Dataset[Person] = ss.createDataset(w)
    //    println(hello.schema)

    //
    //    row2.toDF("a")
    //    val frame = rdd2.toDF()
    //    var schema = StructType(List(
    //      StructField("CUST_CODE", StringType)
    //    ))
    //    ss.createDataFrame(rdd, schema)
    //    println(frame.schema)
    //
    //
    //    println(row2.toDF("a").schema)
  }
}
