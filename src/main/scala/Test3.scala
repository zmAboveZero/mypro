import java.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{HashPartitioner, Partition, SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    //    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setAppName("test").setMaster("local[2]")
    val ss: SparkSession = SparkSession.builder().appName("tt").config("spark.testing.memory", "2147480000").master("local[2]").getOrCreate()


    //    println(ss.conf.get("spark.testing.memory"))
    //    println(ss.conf.get("spark.sql.shuffle.partitions"))
    //    ss.conf.set("spark.sql.shuffle.partitions", 100)
    //    println(ss.conf.get("spark.sql.shuffle.partitions"))
    //
    //    import ss.implicits._
    //    ss.sparkContext.parallelize(List(("A", "1"), ("B", "1"), ("C", "1"), ("C", "1"), ("A", "1"), ("B", "1"), ("A", "1"), ("A", "1")))
    //      .toDF("ID", "b").createTempView("A")
    //    ss.sparkContext.parallelize(List(("A", "1"), ("B", "1"), ("C", "1"), ("C", "1"), ("A", "1"), ("B", "1"), ("A", "1"), ("A", "1")))
    //      .toDF("ID", "b").createTempView("B")
    //    ss.sparkContext.parallelize(List(("A", "1"), ("B", "1"), ("C", "1"), ("C", "1"), ("A", "1"), ("B", "1"), ("A", "1"), ("A", "1")))
    //      .toDF("ID", "b").createTempView("C")
    //    val df_t = ss.sparkContext.parallelize(List(1, 2, 2, 3, 4))
    //    println(df_t.getNumPartitions)
    //    println(df_t.cartesian(df_t).getNumPartitions)
    //    df_t.cartesian(df_t).foreach(println(_))


    val unit1 = ss.sparkContext.parallelize(List(("A", "1"),
      ("B", "1"),
      ("C", "1"),
      ("C", "1"),
      ("A", "1"),
      ("B", "1"),
      ("A", "1"),
      ("A", "1"))).mapValues(_ * 2).map(_.swap).sortByKey()
    //.map(_.swap) //.mapValues(_ * 2)
    val unit2 = ss.sparkContext.parallelize(List(("A", "1"),
      ("B", "1"),
      ("C", "1"),
      ("C", "1"),
      ("A", "1"),
      ("B", "1"),
      ("A", "1"),
      ("A", "1"))).mapValues(_ * 2).map(_.swap).groupByKey() //.map(_.swap) //.mapValues(_ * 2)



    ss.sparkContext.parallelize(List(("A", "1"),
      ("B", "1"),
      ("C", "1"),
      ("C", "1"),
      ("A", "1"),
      ("B", "1"),
      ("A", "1"),
      ("A", "1")))

    val partitions= unit2.partitions

    println(partitions.size)

    println(">>>",unit2.preferredLocations(partitions(0)))
//    partitions.foreach{p=>println(unit2.preferredLocations(p))}




    println("cartesian", unit1.cartesian(unit2).partitioner)
    println("coalesce", unit1.coalesce(10, true).partitioner)
    println("cogroup", unit1.cogroup(unit2).partitioner)
    println("unit1", unit1.partitioner)
    println("partitionBy", unit1.partitionBy(new HashPartitioner(100)).partitioner)
    println("unit2", unit2.partitioner)
    println("subtract", unit1.subtract(unit1).partitioner)


    //    //    val out = df_t.map(ele => (ele._1, ele._2 + 1)).map(_.swap).map(ele => (ele._1 + 1, ele._2))
    //    //    println(df_t.getNumPartitions)
    //    val value = df_t ++ df_t ++ df_t ++ df_t ++ df_t
    //
    //
    //
    //
    //
    //
    //
    //    //.mapPartitionsWithIndex { (part, rows) => rows.map((part, _)) }
    //    hello.foreach(println(_))
    //    println("=====")
    //    val hh = hello.coalesce(5).mapPartitionsWithIndex { (part, row) => row.map((part, _)) }
    //    hh.foreach(println(_))

    //    val rr = value.coalesce(2)


    //    out.dependencies.foreach(println(_))
    //    out.foreach(println(_))
    //    ss.sql(
    //      """
    //        |select * from t1
    //        |union all
    //        |select * from t2
    //        |union all
    //        |select * from t3
    //        |union all
    //        |select * from t4
    //      """.stripMargin).write

    //    ss.sql(
    //      """
    //        |SELECT A.ID
    //        |FROM A
    //        |JOIN B
    //        |ON A.ID=B.ID
    //        |JOIN C
    //        |ON A.ID=C.ID
    //        |WHERE A.ID = 7
    //        |GROUP BY A.ID
    //        |HAVING ID=8
    //      """.stripMargin).explain(true)


    //    println(53 % 5)
    //    println("10000000".hashCode%3)
    //    println("10000001".hashCode%3)
    //    println("10000002".hashCode%3)
    //    println("10000003".hashCode%3)
    //    println("10000004".hashCode%3)
    //    println("10000005".hashCode%3)
    //    println("10000006".hashCode%3)
    //    println("10000007".hashCode%3)
    //    println("20000000".hashCode%3)
    //    println("20000001".hashCode%3)
    //    println("20000002".hashCode%3)
    //    println("20000003".hashCode%3)
    //    println("20000004".hashCode%3)
    //    println("20000005".hashCode%3)
    //    println("20000006".hashCode%3)
    //    println("20000007".hashCode%3)
    //    println("30000000".hashCode%3)
    //    println("30000001".hashCode%3)
    //    println("30000002".hashCode%3)
    //    println("30000003".hashCode%3)
    //    println("30000004".hashCode%3)
    //    println("30000005".hashCode%3)
    //    println("30000006".hashCode)
    //    println("30000007".hashCode%3)
    //    df.map(ele=>ele._1+"\001"+ele._2).saveAsTextFile("C:\\Users\\zm\\Desktop\\testData\\ttttt\\ou")
    //    val rdd2 = rdd1.map((_, 1))
    //    val rdd3 = rdd2.map(e => (e._1, e._2 + 1))
    //    val rdd4 = rdd3.reduceByKey(_ + _)
    //    val rdd5 = rdd4.map(e => (e._1, e._2 - 1))
    //    rdd5.foreach(println(_))
    //    new HashPartitioner(5).getPartition()


    //    println(new Random().nextDouble())
    //    println("fsadfsaf".charAt(1))
  }
}
