//package com.zm.compute
//
//import com.springflute.database.Database
//import com.springflute.udf.RegisterUdf
//import org.apache.spark.sql.SparkSession
//
//object SourceToClean {
//
//  def sourceToClean(ss:SparkSession,crtTime:String,updTime:String) : Unit = {
//
//    //注册要使用的udf函数
//    RegisterUdf.register(ss)
//
//    //1、基本信息表
//    val T_CRMS_D1_CUSTOMER = ss.sql(
//      s"""select
//        |		CUST_CODE				    as	CUST_CODE,
//        |		CUST_TYPE				    as	CUST_TYPE,
//        |		CERT_TYPE				    as	CERT_TYPE,
//        |		CERT_CODE				    as	CERT_CODE,
//        |		NAME				  	    as	NAME,
//        |		BIRTHDAY				    as	BIRTHDAY,
//        |		ETHNIC					    as	ETHNIC,
//        |		NATION					    as	NATION,
//        |		RESIDENCE				    as	RESIDENCE,
//        |		SEX						      as	SEX,
//        |		HEIGHT					    as	HEIGHT,
//        |		DEAD_DATE				    as	DEAD_DATE,
//        |		MARRIED_FLG			    as	MARRIED_FLG,
//        |		CERT_VALID_TERM	    as	CERT_VALID_TERM,
//        |		CERT_START_DATE	    as	CERT_START_DATE,
//        |		CERT_END_DATE		    as	CERT_END_DATE,
//        |		IS_VIP					    as	IS_VIP,
//        |		SECURITY_LEVEL			as	SECURITY_LEVEL,
//        |		verify_TRUE_FLG			as	verify_TRUE_FLG,
//        |		verify_TRUE_DTTM		as	verify_TRUE_DTTM,
//        |		verify_TRUE_SYSCODE	as	verify_TRUE_SYSCODE,
//        |		cleanID(CERT_CODE)		as	CERT_CODE_CLR,
//        |		verifyID(CERT_CODE)		as	CERT_CODE_FLG,
//        |		cleanName(NAME)				  as	NAME_CLR,
//        |		verifyName(NAME)			  as	NAME_FLG,
//        |		'SYS001-CRMS'			  as	SRC_SYS,
//        |		ETL_CRTDTTM				  as	SRC_CRT_TM,
//        |		ETL_UPDDTTM				  as	SRC_UPD_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')	as	ETL_CRT_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')	as	ETL_UPD_TM
//        |from  ${Database.M6}.T_CRMS_D1_CUSTOMER""".stripMargin)
//
//    //2、车辆信息
//    val DRIVERNOTYPE = ss.sql(
//      s"""select
//        |		case  when verifyID(DRIVERNO)='1'  then 'SNA_'+CERTIFICATENO else 'SNAPER_'+ACTUALID+'_'+LOSSITEMENTITYID end	as	CUST_CODE	,
//        |		''					        as	CUST_TYPE,
//        |		DRIVERNOTYPE		    as	CERT_TYPE,
//        |		DRIVERNO			      as	CERT_CODE,
//        |		DRIVERNAME			    as	NAME,
//        |		''					        as	BIRTHDAY,
//        |		''					        as	ETHNIC,
//        |		''					        as	NATION,
//        |		''					        as	RESIDENCE,
//        |		''					        as	SEX,
//        |		''					        as	HEIGHT,
//        |		''					        as	DEAD_DATE,
//        |		''					        as	MARRIED_FLG,
//        |		''					        as	CERT_VALID_TERM,
//        |		''					        as	CERT_START_DATE,
//        |		''					        as	CERT_END_DATE,
//        |		''					        as	IS_VIP,
//        |		''					        as	SECURITY_LEVEL,
//        |		''					        as	verify_TRUE_FLG,
//        |		''					        as	verify_TRUE_DTTM,
//        |		''					        as	verify_TRUE_SYSCODE,
//        |		cleanID(DRIVERNO)		  as	CERT_CODE_CLR,
//        |		verifyID(DRIVERNO)	  as	CERT_CODE_FLG,
//        |		cleanName(DRIVERNAME)	  as	NAME_CLR,
//        |		verifyName(DRIVERNAME)	as	NAME_FLG,
//        |		'SYS002-M6'			    as	SRC_SYS,
//        |		ETL_CRTDTTM			    as	SRC_CRT_TM,
//        |		ETL_UPDDTTM			    as	SRC_UPD_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')			as	ETL_CRT_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')			as	ETL_UPD_TM
//        |from ${Database.M6}.M6_BL_CVT_E02_CAR  t1 left join ${Database.CRMS}.T_CRMS_D1_CUSTOMER t2
//        |on t1.DRIVERNOTYPE=t2.CERT_TYPE and t1.DRIVERNO=t2.CERT_CODE
//        |where  t2.CERT_TYPE=null""".stripMargin)
//    //3、损失信息-人
//    val AL_CF_RP_PERSONLOSSINFO = ss.sql(
//      s"""select
//        |		case when verifyID(CERTIFICATENO)='1'  then 'SNA_'+CERTIFICATENO  else 'SNAPER_'+ACTUALID+'_'+LOSSITEMENTITYID end	as	CUST_CODE,
//        |		''						        as	CUST_TYPE,
//        |		CERTIFICATETYPE			  as	CERT_TYPE,
//        |		CERTIFICATENO		  	  as	CERT_CODE,
//        |		THEINJURYNAME			    as	NAME,
//        |		parse(CERTIFICATENO)	as	BIRTHDAY,
//        |		''						        as	ETHNIC,
//        |		''						        as	NATION,
//        |		''						        as	RESIDENCE,
//        |		SEX						        as	SEX,
//        |		''						        as	HEIGHT,
//        |		''						        as	DEAD_DATE,
//        |		''						        as	MARRIED_FLG,
//        |		''						        as	CERT_VALID_TERM,
//        |		''						        as	CERT_START_DATE,
//        |		''						        as	CERT_END_DATE,
//        |		''						        as	IS_VIP,
//        |		''						        as	SECURITY_LEVEL,
//        |		''						        as	verify_TRUE_FLG,
//        |		''						        as	verify_TRUE_DTTM,
//        |		''						        as	verify_TRUE_SYSCODE,
//        |		cleanID(CERTIFICATENO)	as	CERT_CODE_CLR,
//        |		verifyID(CERTIFICATENO)	as	CERT_CODE_FLG,
//        |		cleanName(THEINJURYNAME)	as	NAME_CLR,
//        |		verifyName(THEINJURYNAME)	as	NAME_FLG,
//        |		'SYS002-M6'				    as	SRC_SYS,
//        |		CREATEDATE				    as	SRC_CRT_TM,
//        |		UPDATEDATE				    as	SRC_UPD_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')				as	ETL_CRT_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')				as	ETL_UPD_TM
//        |from ${Database.M6}.M6_AL_CF_RP_PERSONLOSSINFO t1 left join ${Database.CRMS}.T_CRMS_D1_CUSTOMER
//        |on t1.CERTIFICATETYPE=t2.CERT_TYPE and t1.CERTIFICATENO=t2.CERT_CODE
//        |where t2.CERT_TYPE=null""".stripMargin)
//    //4、账户支付明细
//    val AL_CF_RP_SETTLEMENTACCOUNT = ss.sql(
//      s"""select
//        |		'SNAACCT_'+ACTUALID+'_'+ACCOUNTNO	as	CUST_CODE				,
//        |		''									      as	CUST_TYPE,
//        |		CERTIFICATETYPE			      as	CERT_TYPE,
//        |		CERTIFICATENO				      as	CERT_CODE,
//        |		PAYEEACCOUNTNAME		      as	NAME,
//        |		parse(CERTIFICATENO)      as	BIRTHDAY,
//        |		''									      as	ETHNIC,
//        |		''									      as	NATION,
//        |		''									      as	RESIDENCE,
//        |		parse(CERTIFICATENO)	    as	SEX,
//        |		''									      as	HEIGHT,
//        |		''									      as	DEAD_DATE,
//        |		''									      as	MARRIED_FLG,
//        |		''									      as	CERT_VALID_TERM,
//        |		''									      as	CERT_START_DATE,
//        |		''									      as	CERT_END_DATE,
//        |		''									      as	IS_VIP,
//        |		''									      as	SECURITY_LEVEL,
//        |		''									      as	verify_TRUE_FLG,
//        |		''									      as	verify_TRUE_DTTM,
//        |		''									      as	verify_TRUE_SYSCODE,
//        |		cleanID(CERTIFICATENO)			as	CERT_CODE_CLR,
//        |		verifyID(CERTIFICATENO)			as	CERT_CODE_FLG,
//        |		cleanName(PAYEEACCOUNTNAME)		as	NAME_CLR,
//        |		verifyName(PAYEEACCOUNTNAME)	as	NAME_FLG,
//        |		'SYS002-M6'							  as	SRC_SYS,
//        |		CREATEDATE							  as	SRC_CRT_TM,
//        |		UPDATEDATE							  as	SRC_UPD_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')							as	ETL_CRT_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')							as	ETL_UPD_TM
//        |from ${Database.M6}.M6_AL_CF_RP_SETTLEMENTACCOUNT left join ${Database.CRMS}.T_CRMS_D1_CUSTOMER
//        |on t1.CERTIFICATETYPE=t2.CERT_TYPE and t1.CERTIFICATENO=t2.CERT_CODE
//        |where t2.CERT_TYPE=null""".stripMargin)
//    //5、人员信息
//    val AL_CF_RP_RELATEDPERSONNEL = ss.sql(
//      s"""select
//        |		'SNACLM_'+ACTUALID+'_'+IDNUMBER    as	CUST_CODE,
//        |		CUSTOMERTYPE				  as	CUST_TYPE,
//        |		CERTIFICATETYPE				as	CERT_TYPE,
//        |		CERTIFICATENO				  as	CERT_CODE,
//        |		NAME					      	as	NAME,
//        |		parse(CERTIFICATENO)	as	BIRTHDAY,
//        |		''							      as	ETHNIC,
//        |		NATIONALITY				  	as	NATION,
//        |		ADDRESSOFCERTIFICATE	as	RESIDENCE,
//        |		SEX							      as	SEX,
//        |		''							      as	HEIGHT,
//        |		''							      as	DEAD_DATE,
//        |		''							      as	MARRIED_FLG,
//        |		CERTIFICATEOFLICENSE	as	CERT_VALID_TERM,
//        |		''							      as	CERT_START_DATE,
//        |		''							      as	CERT_END_DATE,
//        |		''							      as	IS_VIP,
//        |		''							      as	SECURITY_LEVEL,
//        |		''							      as	verify_TRUE_FLG,
//        |		''							      as	verify_TRUE_DTTM,
//        |		''							      as	verify_TRUE_SYSCODE,
//        |		cleanID(CERTIFICATENO)	as	CERT_CODE_CLR,
//        |		verifyID(CERTIFICATENO)	as	CERT_CODE_FLG,
//        |		cleanName(NAME)					  as	NAME_CLR,
//        |		verifyName(NAME)				  as	NAME_FLG,
//        |		'SYS002-M6'					  as	SRC_SYS,
//        |		CREATEDATE					  as	SRC_CRT_TM,
//        |		UPDATEDATE					  as	SRC_UPD_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')					as	ETL_CRT_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')					as	ETL_UPD_TM
//        |from ${Database.M6}.M6_AL_CF_RP_RELATEDPERSONNEL t1 left join ${Database.CRMS}.T_CRMS_D1_CUSTOMER
//        |on t1.CERTIFICATETYPE=t2.CERT_TYPE and t1.CERTIFICATENO=t2.CERT_CODE
//        |where t2.CERT_TYPE=null""".stripMargin)
//    //6、人员信息
//    val AL_CF_RP_RELATEDPERSONNEL_1 = ss.sql(
//      s"""select
//        |		'SNACLM_'+ACTUALID+'_'+IDNUMBER    as	CUST_CODE	,
//        |		CUSTOMERTYPE				    as	CUST_TYPE,
//        |		DRIVERTYPE					    as	CERT_TYPE,
//        |		DRIVERLICENSENO				  as	CERT_CODE,
//        |		NAME						        as	NAME,
//        |		parse(CERTIFICATENO)		as	BIRTHDAY,
//        |		''							        as	ETHNIC,
//        |		NATIONALITY				    	as	NATION,
//        |		ADDRESSOFCERTIFICATE		as	RESIDENCE,
//        |		SEX							        as	SEX,
//        |		''							        as	HEIGHT,
//        |		''							        as	DEAD_DATE,
//        |		''							        as	MARRIED_FLG,
//        |		VALIDITYTERMOFLICENSE		as	CERT_VALID_TERM,
//        |		DRIVERLICENSEISSUEDATE	as	CERT_START_DATE,
//        |		VALIDITYTERMOFLICENSE		as	CERT_END_DATE,
//        |		''							        as	IS_VIP,
//        |		''							        as	SECURITY_LEVEL,
//        |		''							        as	verify_TRUE_FLG,
//        |		''							        as	verify_TRUE_DTTM,
//        |		''							        as	verify_TRUE_SYSCODE,
//        |		cleanID(DRIVERLICENSENO)	as	CERT_CODE_CLR,
//        |		verifyID(DRIVERLICENSENO)	as	CERT_CODE_FLG,
//        |		cleanName(NAME)					    as	NAME_CLR,
//        |		verifyName(NAME)				    as	NAME_FLG,
//        |		'SYS002-M6'					    as	SRC_SYS,
//        |		CREATEDATE					    as	SRC_CRT_TM,
//        |		UPDATEDATE					    as	SRC_UPD_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')					as	ETL_CRT_TM,
//        |		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')					as	ETL_UPD_TM
//        |from ${Database.M6}.M6_AL_CF_RP_RELATEDPERSONNEL t1 left join ${Database.CRMS}.T_CRMS_D1_CUSTOMER
//        |on t1.DRIVERTYPE=t2.CERT_TYPE and t1.DRIVERLICENSENO=t2.CERT_CODE
//        |where t2.CERT_TYPE=null""".stripMargin)
//    //合并以上的表并创建view
//    val unionDF = T_CRMS_D1_CUSTOMER
//      .union(DRIVERNOTYPE)
//      .union(AL_CF_RP_PERSONLOSSINFO)
//      .union(AL_CF_RP_SETTLEMENTACCOUNT)
//      .union(AL_CF_RP_RELATEDPERSONNEL)
//      .union(AL_CF_RP_RELATEDPERSONNEL_1)
//    unionDF.createTempView("t1")
//    //按照CUST_CODE分组排序并创建view
//    ss.sql(
//      """select
//        |	CUST_CODE,
//        |	CUST_TYPE,
//        |	CERT_TYPE,
//        |	CERT_CODE,
//        |	NAME,
//        |	BIRTHDAY,
//        |	ETHNIC,
//        |	NATION,
//        |	RESIDENCE,
//        |	SEX,
//        |	HEIGHT,
//        |	DEAD_DATE,
//        |	MARRIED_FLG,
//        |	CERT_VALID_TERM,
//        |	CERT_START_DATE,
//        |	CERT_END_DATE,
//        |	IS_VIP,
//        |	SECURITY_LEVEL,
//        |	verify_TRUE_FLG,
//        |	verify_TRUE_DTTM,
//        |	verify_TRUE_SYSCODE,
//        |	CERT_CODE_CLR,
//        |	CERT_CODE_FLG,
//        |	NAME_CLR,
//        |	NAME_FLG,
//        |	SRC_SYS,
//        |	SRC_CRT_TM,
//        |	SRC_UPD_TM,
//        |	ETL_CRT_TM,
//        |	ETL_UPD_TM  ,
//        |	row_number() over (partition by CUST_CODE order by ETL_UPD_TM desc)  as rn
//        |from t1 """.stripMargin)
//      .createTempView("t2")
//    //选择rn=1的数据表示最新数据
//    var resultDF = ss.sql(
//      s"""insert into ${Database.SNA}.SNA_CLEAN_PERSON
//        |select
//        |	CUST_CODE,
//        |	CUST_TYPE,
//        |	CERT_TYPE,
//        |	CERT_CODE,
//        |	NAME,
//        |	BIRTHDAY,
//        |	ETHNIC,
//        |	NATION,
//        |	RESIDENCE,
//        |	SEX,
//        |	HEIGHT,
//        |	DEAD_DATE,
//        |	MARRIED_FLG,
//        |	CERT_VALID_TERM,
//        |	CERT_START_DATE,
//        |	CERT_END_DATE,
//        |	IS_VIP,
//        |	SECURITY_LEVEL,
//        |	verify_TRUE_FLG,
//        |	verify_TRUE_DTTM,
//        |	verify_TRUE_SYSCODE,
//        |	CERT_CODE_CLR,
//        |	CERT_CODE_FLG,
//        |	NAME_CLR,
//        |	NAME_FLG,
//        |	SRC_SYS,
//        |	SRC_CRT_TM,
//        |	SRC_UPD_TM,
//        |	ETL_CRT_TM,
//        |	ETL_UPD_TM
//        |from t2  where rn=1""".stripMargin)
//
//  }
//}
