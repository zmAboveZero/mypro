package com.zm2

import org.apache.spark.sql.SparkSession


object ETL2 {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[2]").appName("HiveConnApp")
//      .enableHiveSupport()
      .getOrCreate()


    ss.sql(
      """insert overwrite table SNA_CLEAN_PORSON

|select
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
|
|from (
|select
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
|row_number() over (partition by CUST_CODE order by etl_upd_tm )  as rn
|from (
|		--union五个源表
|		--1、基本信息表
|		select
|		''						as	MERGE_ID,
|		CUST_CODE				as	CUST_CODE,
|		CUST_TYPE				as	CUST_TYPE,
|		CERT_TYPE				as	CERT_TYPE,
|		CERT_CODE				as	CERT_CODE,
|		NAME					as	NAME,
|		BIRTHDAY				as	BIRTHDAY,
|		ETHNIC					as	ETHNIC,
|		NATION					as	NATION,
|		RESIDENCE				as	RESIDENCE,
|		SEX						as	SEX,
|		HEIGHT					as	HEIGHT,
|		DEAD_DATE				as	DEAD_DATE,
|		MARRIED_FLG				as	MARRIED_FLG,
|		CERT_VALID_TERM			as	CERT_VALID_TERM,
|		CERT_START_DATE			as	CERT_START_DATE,
|		CERT_END_DATE			as	CERT_END_DATE,
|		IS_VIP					as	IS_VIP,
|		SECURITY_LEVEL			as	SECURITY_LEVEL,
|		verify_TRUE_FLG			as	verify_TRUE_FLG,
|		verify_TRUE_DTTM		as	verify_TRUE_DTTM,
|		verify_TRUE_SYSCODE		as	verify_TRUE_SYSCODE,
|		clean(CERT_CODE)		as	CERT_CODE_CLR,
|		verify(CERT_CODE)		as	CERT_CODE_FLG,
|		clean(NAME)				as	NAME_CLR,
|		verify(NAME)			as	NAME_FLG,
|		'SYS001-CRMS'			as	src_sys,
|		ETL_CRTDTTM				as	src_crt_tm,
|		ETL_UPDDTTM				as	src_upd_tm,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')				as	etl_crt_tm,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')				as	etl_upd_tm
|		from sparkhive.T_CRMS_D1_CUSTOMER
|
|		uninon
|		--2、车辆信息
|		select
|		''					as	MERGE_ID	,
|		case when true 'SNA_'+CERTIFICATENO else 'SNAPER_'+ACTUALID+'_'+LOSSITEMENTITYID end	as	CUST_CODE	,
|		''					as	CUST_TYPE,
|		DRIVERNOTYPE		as	CERT_TYPE,
|		DRIVERNO			as	CERT_CODE,
|		DRIVERNAME			as	NAME,
|		''					as	BIRTHDAY,
|		''					as	ETHNIC,
|		''					as	NATION,
|		''					as	RESIDENCE,
|		''					as	SEX,
|		''					as	HEIGHT,
|		''					as	DEAD_DATE,
|		''					as	MARRIED_FLG,
|		''					as	CERT_VALID_TERM,
|		''					as	CERT_START_DATE,
|		''					as	CERT_END_DATE,
|		''					as	IS_VIP,
|		''					as	SECURITY_LEVEL,
|		''					as	verify_TRUE_FLG,
|		''					as	verify_TRUE_DTTM,
|		''					as	verify_TRUE_SYSCODE,
|		clean(DRIVERNO)		as	CERT_CODE_CLR,
|		verify(DRIVERNO)	as	CERT_CODE_FLG,
|		clean(DRIVERNAME)	as	NAME_CLR,
|		verify(DRIVERNAME)	as	NAME_FLG,
|		'SYS002-M6'			as	src_sys,
|		ETL_CRTDTTM			as	src_crt_tm,
|		ETL_UPDDTTM			as	src_upd_tm,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')			as	etl_crt_tm,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')			as	etl_upd_tm
|		from sparkhive.BL_CVT_E02_CAR  t1 left join sparkhive.T_CRMS_D1_CUSTOMER t2
|		on t1.DRIVERNOTYPE=t2.CERT_TYPE and t1.DRIVERNO=t2.CERT_CODE
|		where  t2.CERT_TYPE=null
|		uninon
|
|		--3、损失信息-人
|		select
|		''				as	MERGE_ID,
|		case when true 'SNA_'+CERTIFICATENO else 'SNAPER_'+ACTUALID+'_'+LOSSITEMENTITYID end	as	CUST_CODE,
|		''						as	CUST_TYPE,,
|		CERTIFICATETYPE			as	CERT_TYPE				,
|		CERTIFICATENO			as	CERT_CODE				,
|		THEINJURYNAME			as	NAME					,
|		parse(CERTIFICATENO)	as	BIRTHDAY				,
|		''						as	ETHNIC					,
|		''						as	NATION					,
|		''						as	RESIDENCE				,
|		SEX						as	SEX						,
|		''						as	HEIGHT					,
|		''						as	DEAD_DATE				,
|		''						as	MARRIED_FLG				,
|		''						as	CERT_VALID_TERM			,
|		''						as	CERT_START_DATE			,
|		''						as	CERT_END_DATE			,
|		''						as	IS_VIP					,
|		''						as	SECURITY_LEVEL			,
|		''						as	verify_TRUE_FLG			,
|		''						as	verify_TRUE_DTTM		,
|		''						as	verify_TRUE_SYSCODE		,
|		clean(CERTIFICATENO)	as	CERT_CODE_CLR			,
|		verify(CERTIFICATENO)	as	CERT_CODE_FLG			,
|		clean(THEINJURYNAME)	as	NAME_CLR				,
|		verify(THEINJURYNAME)	as	NAME_FLG				,
|		'SYS002-M6'				as	src_sys					,
|		CREATEDATE				as	src_crt_tm				,
|		UPDATEDATE				as	src_upd_tm				,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')				as	etl_crt_tm		,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')				as	etl_upd_tm
|		from AL_CF_RP_PERSONLOSSINFO t1 left join sparkhive.T_CRMS_D1_CUSTOMER
|		on t1.CERTIFICATETYPE=t2.CERT_TYPE and t1.CERTIFICATENO=t2.CERT_CODE
|		where t2.CERT_TYPE=null
|		uninon
|
|		--4、账户支付明细
|		select
|		''									as	MERGE_ID				,
|		'SNAACCT_'+ACTUALID+'_'+ACCOUNTNO	as	CUST_CODE				,
|		''									as	CUST_TYPE				,
|		CERTIFICATETYPE						as	CERT_TYPE				,
|		CERTIFICATENO						as	CERT_CODE				,
|		PAYEEACCOUNTNAME					as	NAME					,
|		parse(CERTIFICATENO)				as	BIRTHDAY				,
|		''									as	ETHNIC					,
|		''									as	NATION					,
|		''									as	RESIDENCE				,
|		parse(CERTIFICATENO)				as	SEX						,
|		''									as	HEIGHT					,
|		''									as	DEAD_DATE				,
|		''									as	MARRIED_FLG				,
|		''									as	CERT_VALID_TERM			,
|		''									as	CERT_START_DATE			,
|		''									as	CERT_END_DATE			,
|		''									as	IS_VIP					,
|		''									as	SECURITY_LEVEL			,
|		''									as	verify_TRUE_FLG			,
|		''									as	verify_TRUE_DTTM		,
|		''									as	verify_TRUE_SYSCODE		,
|		clean(CERTIFICATENO)				as	CERT_CODE_CLR			,
|		verify(CERTIFICATENO)				as	CERT_CODE_FLG			,
|		clean(PAYEEACCOUNTNAME)				as	NAME_CLR				,
|		verify(PAYEEACCOUNTNAME)			as	NAME_FLG				,
|		'SYS002-M6'							as	src_sys					,
|		CREATEDATE							as	src_crt_tm				,
|		UPDATEDATE							as	src_upd_tm				,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')							as	etl_crt_tm	,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')							as	etl_upd_tm
|		from AL_CF_RP_SETTLEMENTACCOUNT left join sparkhive.T_CRMS_D1_CUSTOMER
|		on t1.CERTIFICATETYPE=t2.CERT_TYPE and t1.CERTIFICATENO=t2.CERT_CODE
|		where t2.CERT_TYPE=null
|		uninon
|
|		--5、人员信息
|		select
|		''						as	MERGE_ID				,
|		'SNACLM_'+ACTUALID+'_'+IDNUMBER    as	CUST_CODE	,
|		CUSTOMERTYPE				as	CUST_TYPE				,
|		CERTIFICATETYPE				as	CERT_TYPE				,
|		CERTIFICATENO				as	CERT_CODE				,
|		NAME						as	NAME					,
|		parse(CERTIFICATENO)		as	BIRTHDAY				,
|		''							as	ETHNIC					,
|		NATIONALITY					as	NATION					,
|		ADDRESSOFCERTIFICATE		as	RESIDENCE				,
|		SEX							as	SEX						,
|		''							as	HEIGHT					,
|		''							as	DEAD_DATE				,
|		''							as	MARRIED_FLG				,
|		CERTIFICATEOFLICENSE		as	CERT_VALID_TERM			,
|		''							as	CERT_START_DATE			,
|		''							as	CERT_END_DATE			,
|		''							as	IS_VIP					,
|		''							as	SECURITY_LEVEL			,
|		''							as	verify_TRUE_FLG			,
|		''							as	verify_TRUE_DTTM		,
|		''							as	verify_TRUE_SYSCODE		,
|		clean(CERTIFICATENO)		as	CERT_CODE_CLR			,
|		verify(CERTIFICATENO)		as	CERT_CODE_FLG			,
|		clean(NAME)					as	NAME_CLR				,
|		verify(NAME)				as	NAME_FLG				,
|		'SYS002-M6'					as	src_sys					,
|		CREATEDATE					as	src_crt_tm				,
|		UPDATEDATE					as	src_upd_tm				,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')					as	etl_crt_tm ,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')					as	etl_upd_tm
|		from AL_CF_RP_RELATEDPERSONNEL t1 left join sparkhive.T_CRMS_D1_CUSTOMER
|		on t1.CERTIFICATETYPE=t2.CERT_TYPE and t1.CERTIFICATENO=t2.CERT_CODE
|		where t2.CERT_TYPE=null
|		uninon
|		--6、人员信息
|		select
|		''							as	MERGE_ID				,
|		'SNACLM_'+ACTUALID+'_'+IDNUMBER    as	CUST_CODE	,
|		CUSTOMERTYPE				as	CUST_TYPE				,
|		DRIVERTYPE					as	CERT_TYPE				,
|		DRIVERLICENSENO				as	CERT_CODE				,
|		NAME						as	NAME					,
|		parse(CERTIFICATENO)		as	BIRTHDAY				,
|		''							as	ETHNIC					,
|		NATIONALITY					as	NATION					,
|		ADDRESSOFCERTIFICATE		as	RESIDENCE				,
|		SEX							as	SEX						,
|		''							as	HEIGHT					,
|		''							as	DEAD_DATE				,
|		''							as	MARRIED_FLG				,
|		VALIDITYTERMOFLICENSE		as	CERT_VALID_TERM			,
|		DRIVERLICENSEISSUEDATE		as	CERT_START_DATE			,
|		VALIDITYTERMOFLICENSE		as	CERT_END_DATE			,
|		''							as	IS_VIP					,
|		''							as	SECURITY_LEVEL			,
|		''							as	verify_TRUE_FLG			,
|		''							as	verify_TRUE_DTTM		,
|		''							as	verify_TRUE_SYSCODE		,
|		clean(DRIVERLICENSENO)		as	CERT_CODE_CLR			,
|		verify(DRIVERLICENSENO)		as	CERT_CODE_FLG			,
|		clean(NAME)					as	NAME_CLR				,
|		verify(NAME)				as	NAME_FLG				,
|		'SYS002-M6'					as	src_sys					,
|		CREATEDATE					as	src_crt_tm				,
|		UPDATEDATE					as	src_upd_tm
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')					as	etl_crt_tm		,
|		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')					as	etl_upd_tm
|		from AL_CF_RP_RELATEDPERSONNEL t1 left join sparkhive.T_CRMS_D1_CUSTOMER
|		on t1.DRIVERTYPE=t2.CERT_TYPE and t1.DRIVERLICENSENO=t2.CERT_CODE
|		where t2.CERT_TYPE=null
|) temp
|) where rn=1 """.stripMargin)













  }

}
