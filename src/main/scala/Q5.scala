import org.apache.spark.sql.SparkSession

object Q5 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[*]").appName("quiz").getOrCreate()
    //Q5: 请输出总量数据下，存在登陆数据的各个province中，登陆时间最早的前3人及对应的登陆时间，若不满3人，需要留空。
    //输出结构为 province，account_id_1, login_time_1, account_id_2, login_time_2, account_id_3, login_time_3
    ss.sql(
      """
        |select
        |province as province,
        |nvl(split(collect_list(tuple)[0],'\\|')[0],'') as account_id_1,
        |nvl(split(collect_list(tuple)[0],'\\|')[1],'') as login_time_1,
        |nvl(split(collect_list(tuple)[1],'\\|')[0],'') as account_id_2,
        |nvl(split(collect_list(tuple)[1],'\\|')[1],'') as login_time_2,
        |nvl(split(collect_list(tuple)[2],'\\|')[0],'') as account_id_3,
        |nvl(split(collect_list(tuple)[2],'\\|')[1],'') as login_time_3
        |from ( select province,concat_ws('|',account_id,earliest_logtime) as tuple,
        |              row_number() over(partition by province order by earliest_logtime )  as rn
        |       from (select  province,account_id,min(logtime) as earliest_logtime
        |             from login_data_province
        |             where trim(nvl(account_id,''))<>''
        |             and trim(nvl(logtime,''))<>''
        |             group by province,account_id) t
        |      ) t1
        |where rn<=3
        |group by province
      """.stripMargin).show()
  }

}
