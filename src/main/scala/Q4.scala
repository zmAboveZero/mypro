import org.apache.spark.sql.SparkSession

object Q4 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[*]").appName("quiz").getOrCreate()

    //Q4: 请输出每个分区下，每个province的去重登陆人数。输出结构为 pt，province，cnt_login
    ss.sql(
      """
        |select dt,province,count(distinct account_id) as cnt_login
        |from quiz.login_data_province t
        |group by dt ,province
      """.stripMargin).show()

  }

}
