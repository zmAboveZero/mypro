package com.test

import java.io.Serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object Test1 {

  case class Person(name: String, age: Int) extends Serializable

  case class Student(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().appName("test").master("local[1]").getOrCreate()
    var sc = ss.sparkContext
    var hell = sc.parallelize(Array(Student("tom", 2), Student("jerry", 5), Student("jack", 6)))


    import ss.implicits._

    val ints = List(1, 3, 4, 5, 65)
    val ints1: Dataset[Int] = ints.toDS()
    val ints2: DataFrame = ints.toDF()


    var input = List(Person("tom", 1), Person("jerry", 5), Person("jack", 8))

    println(input.toDF().getClass)
    //    println(input.isInstanceOf[DatasetHolder[]])

    //创建DF和DS
    var df = input.toDF()
    var ds = input.toDS()


    //DF和DS到RDD的转换和转换后的RDD类型
    val dfRDD: RDD[Row] = df.rdd
    val dsRDD: RDD[Person] = ds.rdd


    //DF和DS的类型
    df.map(e => e.getAs[String]("name"))
    ds.map(e => e.name)

    //DF与DS之间的相互转换,由于DF的类型就是Row，DS是强类型(什么类型都可以装)
    //所以DS到DS的转换要指定一个类型[Person]
    //DS到DF的转换就直接转换就可以
    val ds_df: DataFrame = ds.toDF()
    val df_ds: Dataset[Person] = df.as[Person]

    //转换后就可以正常按照自己的类型使用了。
    ds_df.map(e => e.getAs[String]("name"))
    df_ds.map(e => e.name)










    //    println(ss.udf)
    //    hell.toDF().foreach(e => println(e.getClass))
    //    hell.toDF().printSchema()
    //
    //
    //    hell.toDS.foreach(e => println(e.getClass))
    //    hell.toDS.printSchema()
    //
    //
    //    println("+++++++++++++++++++++")
    //    //    val haha: RDD[Person] = hell.map(e => Person(e._1, e._2))
    //    //    haha.toDF()
    //    val array = List(Person(1, "tank1", 25), Person(2, "tank2", 26), Person(3, "tank3", 29))

    //
    //    val df: DataFrame = sc.parallelize(array).toDF("id", "name", "age")
    //    val rdd1 = df.rdd
    //    df.foreach(e => println(e.getClass))
    //    println(df.schema)
    //    df.printSchema()
    //    println("=========")
    //    //    val array1 = List(Person(1, "tank1", 25), Person(2, "tank2", 26), Person(3, "tank3", 29))
    //    val ds = sc.parallelize(array).toDS()
    //    val rdd2 = ds.rdd
    //    ds.foreach(e => println(e.getClass))
    //    println(ds.schema)
    //    ds.printSchema()
    //
    //    ds.select("name").show()
    //
    //
    //    println("==============================")
    //
    //
    //    var tt = List(Row(1, "", 3), Row(4, "", 6), Row(7, "", 9))
    //    var hah = sc.parallelize(tt)
    //    hah.foreach(e => println(e.getClass))
    //    //    var a = List((1, 2, 3), (4, 5, 6), (7, 8, 9))
    //    //    hah.toDF().rdd.foreach(e => println(e.getClass))
    //


  }

}
