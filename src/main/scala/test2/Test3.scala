package test2

import com.zm2.tables.T3help.SNA_HELP_PERSON
import org.apache.spark.sql.SparkSession

object Test3 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .config("spark.testing.memory", "2147480000")
      .appName("testhive")
      .master("local[2]")
      .getOrCreate()

    SNA_HELP_PERSON.getTable(ss).createTempView("SNA_HELP_PERSON")
    var t1 = ss.sql(
      """
        |select
        |KEY,
        |MERGE_ID,
        |CUST_CODE,
        |CUST_TYPE,
        |CERT_TYPE,
        |CERT_CODE,
        |NAME,
        |BIRTHDAY,
        |ETHNIC,
        |NATION,
        |RESIDENCE,
        |SEX,
        |HEIGHT,
        |DEAD_DATE,
        |MARRIED_FLG,
        |CERT_VALID_TERM,
        |CERT_START_DATE,
        |CERT_END_DATE,
        |IS_VIP,
        |SECURITY_LEVEL,
        |CHECK_TRUE_FLG,
        |CHECK_TRUE_DTTM,
        |CHECK_TRUE_SYSCODE,
        |CERT_CODE_CLR,
        |CERT_CODE_FLG,
        |NAME_CLR,
        |NAME_FLG,
        |src_sys,
        |src_crt_tm,
        |src_upd_tm,
        |etl_crt_tm,
        |ETL_UPD_TM
        |from
        |(
        |select
        |KEY,
        |MERGE_ID,
        |CUST_CODE,
        |CUST_TYPE,
        |CERT_TYPE,
        |CERT_CODE,
        |NAME,
        |BIRTHDAY,
        |ETHNIC,
        |NATION,
        |RESIDENCE,
        |SEX,
        |HEIGHT,
        |DEAD_DATE,
        |MARRIED_FLG,
        |CERT_VALID_TERM,
        |CERT_START_DATE,
        |CERT_END_DATE,
        |IS_VIP,
        |SECURITY_LEVEL,
        |CHECK_TRUE_FLG,
        |CHECK_TRUE_DTTM,
        |CHECK_TRUE_SYSCODE,
        |CERT_CODE_CLR,
        |CERT_CODE_FLG,
        |NAME_CLR,
        |NAME_FLG,
        |src_sys,
        |src_crt_tm,
        |src_upd_tm,
        |etl_crt_tm,
        |ETL_UPD_TM,
        |row_number() over(partition by MERGE_ID order by ETL_UPD_TM) rn
        |from SNA_HELP_PERSON   where src_sys="CRMS"
        |) temp where temp.rn=1
      """.stripMargin)

    var t2 = ss.sql(
      """
        |select
        |KEY,
        |MERGE_ID,
        |CUST_CODE,
        |CUST_TYPE,
        |CERT_TYPE,
        |CERT_CODE,
        |NAME,
        |BIRTHDAY,
        |ETHNIC,
        |NATION,
        |RESIDENCE,
        |SEX,
        |HEIGHT,
        |DEAD_DATE,
        |MARRIED_FLG,
        |CERT_VALID_TERM,
        |CERT_START_DATE,
        |CERT_END_DATE,
        |IS_VIP,
        |SECURITY_LEVEL,
        |CHECK_TRUE_FLG,
        |CHECK_TRUE_DTTM,
        |CHECK_TRUE_SYSCODE,
        |CERT_CODE_CLR,
        |CERT_CODE_FLG,
        |NAME_CLR,
        |NAME_FLG,
        |src_sys,
        |src_crt_tm,
        |src_upd_tm,
        |etl_crt_tm,
        |ETL_UPD_TM
        |from
        |(
        |select
        |KEY,
        |MERGE_ID,
        |CUST_CODE,
        |CUST_TYPE,
        |CERT_TYPE,
        |CERT_CODE,
        |NAME,
        |BIRTHDAY,
        |ETHNIC,
        |NATION,
        |RESIDENCE,
        |SEX,
        |HEIGHT,
        |DEAD_DATE,
        |MARRIED_FLG,
        |CERT_VALID_TERM,
        |CERT_START_DATE,
        |CERT_END_DATE,
        |IS_VIP,
        |SECURITY_LEVEL,
        |CHECK_TRUE_FLG,
        |CHECK_TRUE_DTTM,
        |CHECK_TRUE_SYSCODE,
        |CERT_CODE_CLR,
        |CERT_CODE_FLG,
        |NAME_CLR,
        |NAME_FLG,
        |src_sys,
        |src_crt_tm,
        |src_upd_tm,
        |etl_crt_tm,
        |ETL_UPD_TM,
        |row_number() over(partition by MERGE_ID order by ETL_UPD_TM) rn
        |from SNA_HELP_PERSON   where src_sys<>"CRMS"
        |) temp where temp.rn=1
        |
        |
      """.stripMargin)


    t1.createTempView("CRMS")
    t2.createTempView("OTHER")

    ss.sql(
      """
        |
        |select
        |case when  t1.MERGE_ID is null then t2.MERGE_ID else t1.MERGE_ID end  as MERGE_ID,
        | t2.MERGE_ID,
        | t2.NAME
        |from CRMS t1
        |full outer join OTHER t2
        |on t1.MERGE_ID=t2.MERGE_ID
        |
        |
      """.stripMargin).show()

  }

}
