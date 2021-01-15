package com.zm.compute

import org.apache.spark.sql.SparkSession

object etl {
  def main(args: Array[String]): Unit = {

    var ss = SparkSession.builder()
      .appName("AnalyseGraph")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    var T_CRMS_D1_CUSTOMER = ss.sql(
      """
        |select
        |CUST_CODE as CUST_CODE,
        |CUST_TYPE as CUST_TYPE,
        |CERT_TYPE as CERT_TYPE,
        |CERT_CODE as CERT_CODE,
        |NAME as NAME,
        |BIRTHDAY as BIRTHDAY,
        |ETHNIC as ETHNIC,
        |NATION as NATION,
        |RESIDENCE as RESIDENCE,
        |SEX as SEX,
        |HEIGHT as HEIGHT,
        |DEAD_DATE as DEAD_DATE,
        |MARRIED_FLG as MARRIED_FLG,
        |CERT_VALID_TERM as CERT_VALID_TERM,
        |CERT_START_DATE as CERT_START_DATE,
        |CERT_END_DATE as CERT_END_DATE,
        |IS_VIP as IS_VIP,
        |SECURITY_LEVEL as SECURITY_LEVEL,
        |'' as verify_TRUE_FLG,
        |'' as verify_TRUE_DTTM,
        |'' as verify_TRUE_SYSCODE,
        |'' as CERT_CODE_CLR,
        |'' as CERT_CODE_FLG,
        |'' as NAME_CLR,
        |'' as NAME_FLG,
        |'SYS001-CRMS' as src_sys,
        |ETL_CRTDTTM as src_crt_tm,
        |ETL_UPDDTTM as src_upd_tm,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_crt_tm,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_upd_tm
        |from zm.T_CRMS_D1_CUSTOMER
        |
      """.stripMargin)
    var M6_BL_CVT_E02_CAR = ss.sql(
      """
        |
        |select
        |'' as CUST_CODE	,
        |'' as CUST_TYPE,
        |DRIVERNOTYPE as CERT_TYPE,
        |DRIVERNO as CERT_CODE,
        |DRIVERNAME as NAME,
        |'' as BIRTHDAY,
        |'' as ETHNIC,
        |'' as NATION,
        |'' as RESIDENCE,
        |'' as SEX,
        |'' as HEIGHT,
        |'' as DEAD_DATE,
        |'' as MARRIED_FLG,
        |'' as CERT_VALID_TERM,
        |'' as CERT_START_DATE,
        |'' as CERT_END_DATE,
        |'' as IS_VIP,
        |'' as SECURITY_LEVEL,
        |'' as verify_TRUE_FLG,
        |'' as verify_TRUE_DTTM,
        |'' as verify_TRUE_SYSCODE,
        |'' as CERT_CODE_CLR,
        |'' as CERT_CODE_FLG,
        |'' as NAME_CLR,
        |'' as NAME_FLG,
        |'SYS002-M6' as src_sys,
        |ETL_CRTDTTM as src_crt_tm,
        |ETL_UPDDTTM as src_upd_tm,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_crt_tm,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_upd_tm
        |from zm.M6_BL_CVT_E02_CAR  t1 left join zm.T_CRMS_D1_CUSTOMER t2
        |on t1.DRIVERNOTYPE=t2.CERT_TYPE and t1.DRIVERNO=t2.CERT_CODE
        |where  t2.CERT_TYPE=null
        |
  """.stripMargin)

    var M6_AL_CF_RP_PERSONLOSSINFO = ss.sql(
      """
        |select
        |'' as CUST_CODE,
        |'' as CUST_TYPE,
        |CERTIFICATETYPE as CERT_TYPE ,
        |CERTIFICATENO as CERT_CODE ,
        |THEINJURYNAME as NAME ,
        |'' as BIRTHDAY ,
        |'' as ETHNIC ,
        |'' as NATION ,
        |'' as RESIDENCE ,
        |t1.SEX as SEX ,
        |'' as HEIGHT ,
        |'' as DEAD_DATE ,
        |'' as MARRIED_FLG ,
        |'' as CERT_VALID_TERM ,
        |'' as CERT_START_DATE ,
        |'' as CERT_END_DATE ,
        |'' as IS_VIP ,
        |'' as SECURITY_LEVEL ,
        |'' as verify_TRUE_FLG ,
        |'' as verify_TRUE_DTTM ,
        |'' as verify_TRUE_SYSCODE ,
        |'' as CERT_CODE_CLR ,
        |'' as CERT_CODE_FLG ,
        |'' as NAME_CLR ,
        |'' as NAME_FLG ,
        |'SYS002-M6' as src_sys ,
        |CREATEDATE as src_crt_tm ,
        |UPDATEDATE as src_upd_tm ,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_crt_tm ,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_upd_tm
        |from zm.M6_AL_CF_RP_PERSONLOSSINFO t1 left join zm.T_CRMS_D1_CUSTOMER t2
        |on t1.CERTIFICATETYPE=t2.CERT_TYPE and t1.CERTIFICATENO=t2.CERT_CODE
        |where t2.CERT_TYPE=null
        |
        |
      """.stripMargin)
    var M6_AL_CF_RP_SETTLEMENTACCOUNT = ss.sql(
      """
        |select
        |'' as CUST_CODE ,
        |'' as CUST_TYPE ,
        |CERTIFICATETYPE as CERT_TYPE ,
        |CERTIFICATENO as CERT_CODE ,
        |PAYEEACCOUNTNAME as NAME ,
        |'' as BIRTHDAY ,
        |'' as ETHNIC ,
        |'' as NATION ,
        |'' as RESIDENCE ,
        |'' as SEX ,
        |'' as HEIGHT ,
        |'' as DEAD_DATE ,
        |'' as MARRIED_FLG ,
        |'' as CERT_VALID_TERM ,
        |'' as CERT_START_DATE ,
        |'' as CERT_END_DATE ,
        |'' as IS_VIP ,
        |'' as SECURITY_LEVEL ,
        |'' as verify_TRUE_FLG ,
        |'' as verify_TRUE_DTTM ,
        |'' as verify_TRUE_SYSCODE ,
        |'' as CERT_CODE_CLR ,
        |'' as CERT_CODE_FLG ,
        |'' as NAME_CLR ,
        |'' as NAME_FLG ,
        |'SYS002-M6' as src_sys ,
        |CREATEDATE as src_crt_tm ,
        |UPDATEDATE as src_upd_tm ,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_crt_tm	,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_upd_tm
        |from zm.M6_AL_CF_RP_SETTLEMENTACCOUNT  t1 left join zm.T_CRMS_D1_CUSTOMER t2
        |on t1.CERTIFICATETYPE=t2.CERT_TYPE and t1.CERTIFICATENO=t2.CERT_CODE
        |where t2.CERT_TYPE=null
        |
        |
      """.stripMargin)
    var M6_AL_CF_RP_RELATEDPERSONNEL = ss.sql(
      """
        |
        |select
        |'' as CUST_CODE ,
        |t1.CUSTOMERTYPE as CUST_TYPE ,
        |t1.CERTIFICATETYPE as CERT_TYPE ,
        |t1.CERTIFICATENO as CERT_CODE ,
        |t1.NAME as NAME ,
        |'' as BIRTHDAY ,
        |'' as ETHNIC ,
        |t1.NATIONALITY as NATION ,
        |t1.ADDRESSOFCERTIFICATE as RESIDENCE ,
        |t1.SEX as SEX ,
        |'' as HEIGHT ,
        |'' as DEAD_DATE ,
        |'' as MARRIED_FLG ,
        |t1.CERTIFICATEOFLICENSE as CERT_VALID_TERM ,
        |'' as CERT_START_DATE ,
        |'' as CERT_END_DATE ,
        |'' as IS_VIP ,
        |'' as SECURITY_LEVEL ,
        |'' as verify_TRUE_FLG ,
        |'' as verify_TRUE_DTTM ,
        |'' as verify_TRUE_SYSCODE ,
        |'' as CERT_CODE_CLR ,
        |'' as CERT_CODE_FLG ,
        |'' as NAME_CLR ,
        |'' as NAME_FLG ,
        |'SYS002-M6' as src_sys ,
        |CREATEDATE as src_crt_tm ,
        |UPDATEDATE as src_upd_tm ,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_crt_tm ,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_upd_tm
        |from zm.M6_AL_CF_RP_RELATEDPERSONNEL t1 left join zm.T_CRMS_D1_CUSTOMER t2
        |on t1.CERTIFICATETYPE=t2.CERT_TYPE and t1.CERTIFICATENO=t2.CERT_CODE
        |where t2.CERT_TYPE=null
        |
      """.stripMargin)

    var M6_AL_CF_RP_RELATEDPERSONNEL1 = ss.sql(
      """
        |select
        |'' as CUST_CODE	,
        |t1.CUSTOMERTYPE as CUST_TYPE ,
        |t1.DRIVERTYPE as CERT_TYPE ,
        |t1.DRIVERLICENSENO as CERT_CODE ,
        |t1.NAME as NAME ,
        |'' as BIRTHDAY ,
        |'' as ETHNIC ,
        |t1.NATIONALITY as NATION ,
        |t1.ADDRESSOFCERTIFICATE as RESIDENCE ,
        |t1.SEX as SEX ,
        |'' as HEIGHT ,
        |'' as DEAD_DATE ,
        |'' as MARRIED_FLG ,
        |t1.VALIDITYTERMOFLICENSE as CERT_VALID_TERM ,
        |t1.DRIVERLICENSEISSUEDATE as CERT_START_DATE ,
        |t1.VALIDITYTERMOFLICENSE as CERT_END_DATE ,
        |'' as IS_VIP ,
        |'' as SECURITY_LEVEL ,
        |'' as verify_TRUE_FLG ,
        |'' as verify_TRUE_DTTM ,
        |'' as verify_TRUE_SYSCODE ,
        |'' as CERT_CODE_CLR ,
        |'' as CERT_CODE_FLG ,
        |'' as NAME_CLR ,
        |'' as NAME_FLG ,
        |'SYS002-M6' as src_sys ,
        |CREATEDATE as src_crt_tm ,
        |UPDATEDATE as src_upd_tm,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_crt_tm ,
        |from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_upd_tm
        |from zm.M6_AL_CF_RP_RELATEDPERSONNEL t1 left join zm.T_CRMS_D1_CUSTOMER t2
        |on t1.DRIVERTYPE=t2.CERT_TYPE and t1.DRIVERLICENSENO=t2.CERT_CODE
        |where t2.CERT_TYPE=null
      """.stripMargin)

    var union_df = T_CRMS_D1_CUSTOMER
      .union(M6_BL_CVT_E02_CAR)
      .union(M6_AL_CF_RP_PERSONLOSSINFO)
      .union(M6_AL_CF_RP_SETTLEMENTACCOUNT)
      .union(M6_AL_CF_RP_RELATEDPERSONNEL)
      .union(M6_AL_CF_RP_RELATEDPERSONNEL1)
    union_df.createTempView("union_df")
    var temp_df = ss.sql(
      """
        |select
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
        |verify_TRUE_FLG,
        |verify_TRUE_DTTM,
        |verify_TRUE_SYSCODE,
        |CERT_CODE_CLR,
        |CERT_CODE_FLG,
        |NAME_CLR,
        |NAME_FLG,
        |src_sys,
        |src_crt_tm,
        |src_upd_tm,
        |etl_crt_tm,
        |etl_upd_tm  ,
        |row_number() over (partition by CUST_CODE order by etl_upd_tm desc)  as rn
        |from union_df
      """.stripMargin)
    temp_df.createTempView("temp_df")
    var re_df = ss.sql(
      """
        |select
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
        |verify_TRUE_FLG,
        |verify_TRUE_DTTM,
        |verify_TRUE_SYSCODE,
        |CERT_CODE_CLR,
        |CERT_CODE_FLG,
        |NAME_CLR,
        |NAME_FLG,
        |src_sys,
        |src_crt_tm,
        |src_upd_tm,
        |etl_crt_tm,
        |etl_upd_tm
        |from temp_df
        |where rn=1
      """.stripMargin)


  }

}
