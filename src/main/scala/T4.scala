import org.apache.spark.sql.SparkSession

object T4 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
    import ss.implicits._
    ss.sparkContext.parallelize(List(1, 2, 3, 4, 5)).toDF("f").show()
    ss.sparkContext.parallelize(List((1, 2), (2, 3), (5, 6))).toDF("a", "b").show()
    ss.sparkContext.parallelize(List((1, 2), (2, 3), (5, 6))).toDF("a", "b").createTempView("t1")
    ss.sparkContext.parallelize(List((1, 2), (2, 3), (5, 6))).toDF("a", "b").createTempView("t2")

    ss.sql(
      """
        |select * from t1  join t2  on t1.a=t2.a
        |""".stripMargin).show()

  }

}
