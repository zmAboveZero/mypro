package quiz

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

object Q3 {


  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[*]").appName("quiz").getOrCreate()
    //Q3: 通过Q1，Q2加载的数据，将用户登陆表中的ip转化为对应的国家地区并落表（避免笛卡尔积）
    //---------------------------------------->
    //---------------------------------------->
    //一时想不出来除笛卡尔积，其他的实现方式，
    //所以还是用了笛卡尔积，希望是小表，可以mapJoin的那种
    // 另外一种就是用广播变量了，类似mapJoin
    // 惭愧惭愧。。。。
    sql(ss)
    broadcast(ss)
  }

  def sql(ss: SparkSession): Unit = {
    ss.sql(
      """
        |select t1.account_id,t1.logtime,t1.dt,t1.long_ip,t2.province
        |from ( select
        |       logtime,
        |       account_id,
        |       dt,
        |       cast(split(ip, '\\.')[0]*256*256*256+
        |            split(ip, '\\.')[1]*256*256+
        |            split(ip, '\\.')[2]*256+
        |            split(ip, '\\.')[3] as bigint)  as long_ip
        |       from quiz.login_data  distribute by rand()) t1
        |join quiz.ip_china t2
        |on 1=1
        |where t2.long_ip_start<t1.long_ip
        |and t1.long_ip <t2.long_ip_end
      """.stripMargin).repartition(1)
      .write.insertInto("quiz.login_data_province")

  }

  def broadcast(ss: SparkSession): Unit = {
    val ip_data = ss.table("quiz.ip_china").select("long_ip_start", "long_ip_end", "province")
      .rdd.map(row => (row.getAs[String]("long_ip_start"),
      row.getAs[String]("long_ip_end"),
      row.getAs[String]("province"))).collect()
    val bdata = ss.sparkContext.broadcast(ip_data)
    val login_data_df = ss.table("quiz.login_data")
    val with_province = login_data_df
      .rdd.map { row =>
      val long_ip = row.getAs[String]("ip").toLong
      var flag = true
      var province = ""
      for (elem <- bdata.value) {
        if (elem._1.toLong < long_ip && long_ip < elem._2.toLong) {
          province = elem._3
          flag = false
        }
      }
      Row.fromSeq(row.toSeq +: province)
    }
    ss.createDataFrame(with_province, login_data_df.schema.add("province", StringType))
      .write.insertInto("quiz.login_data_province")
  }
}
