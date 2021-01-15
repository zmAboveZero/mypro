import org.apache.spark.sql.SparkSession

object Test10 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .config("spark.testing.memory", "2147480000")
      .appName("testSql")
      .master("local[2]")
      .getOrCreate()
    import ss.implicits._
    ss.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6)).toDF("f").createTempView("tmp1")
    //    ss.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6)).toDF("f").createTempView("tmp2")
    //    ss.sql(
    //      """
    //        |select * from tmp1
    //      """.stripMargin).explain(true)
    ss.sql(
      """
        |select * from tmp1
      """.stripMargin).show()


    // println(ss.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6)).toDF("f").toString())
    //    try {
    //      ss.sql(
    //        """
    //          |select t1.f as t1,t2.f as t2
    //          |from tmp1 t1
    //          |left join tmp2 t2
    //          |on t1.f=t2.f
    //          |-- and t3.f=3
    //          |-- and t1a.f in (3,4,5)
    //          |and t2.f in (4,5,6)
    //          |where t1.f in (3,4,5)
    //        """.stripMargin).show()
    //    } catch {
    //      case e: Exception => 88
    //    }
    //    ss.sparkContext.stop()
    //    ss.sql(
    //      """
    //        |select t1.f as t1,t2.f as t2
    //        |from tmp1 t1
    //        |left join tmp2 t2
    //        |on t1.f=t2.f
    //        |-- and t3.f=3
    //        |-- and t1a.f in (3,4,5)
    //        |and t2.f in (4,5,6)
    //        |where t1.f in (3,4,5)
    //      """.stripMargin).show()

    //
    //    val ss1 = SparkSession.builder()
    //      .config("spark.testing.memory", "2147480000")
    //      .appName("testSql")
    //      .master("local[2]")
    //      .getOrCreate()
    //    ss1.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6)).foreach(println(_))
    //    ss1.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6))
    //    ss1.sql(
    //      """
    //        |select t1.f as t1,t2.f as t2
    //        |from tmp1 t1
    //        |left join tmp2 t2
    //        |on t1.f=t2.f
    //        |-- and t2.f=3
    //        |-- and t1.f in (3,4,5)
    //        |and t2.f in (4,5,6)
    //        |where t1.f in (3,4,5)
    //      """.stripMargin).explain()
  }

}
