package com.GraphComputer.utils

import org.apache.spark.graphx.Edge
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object ImportData {
  val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
  val sc = new SparkContext(conf)

  def importEdges() = {
    val edgesDir = "C:\\Users\\zm\\Desktop\\graphInputData\\Relation\\"
    var items = Array("REL_PERSON_TEL.csv",
      "REL_PERSON_ACCOUNT.csv",
      "REL_PERSON_CASE.csv",
      "SNA_REL_CORP_POLICY.csv",
      "SNA_REL_CORP_PERSON.csv",
      "SNA_REL_CORP_CAR.csv",
      "REL_CAR_PERSON.csv",
      "REL_CAR_CASE.csv",
      "REL_CAR_POLICY.csv",
      "REL_CASE_TEL.csv"
    )
    var rel_RDD = ArrayBuffer[RDD[(Edge[String])]]()
    for (i <- items) {
      var label = i match {
        case "REL_PERSON_TEL.csv" => "REL009"
        case "REL_PERSON_ACCOUNT.csv" => "REL010"
        case "REL_PERSON_CASE.csv" => "REL005"
        case "SNA_REL_CORP_POLICY.csv" => "unkown"
        case "SNA_REL_CORP_PERSON.csv" => "unkown"
        case "SNA_REL_CORP_CAR.csv" => "REL016"
        case "REL_CAR_PERSON.csv" => "REL001"
        case "REL_CAR_CASE.csv" => "REL011"
        case "REL_CAR_POLICY.csv" => "REL013"
        case "REL_CASE_TEL.csv" => "REL019"
      }

      rel_RDD += GraphUtil.toRel(sc.textFile(edgesDir + i), label)
    }
    sc.union(rel_RDD)
  }


  def importVetexes() = {
    val vertexesDir = "C:\\Users\\zm\\Desktop\\graphInputData\\Entity\\"
    val items = Array("SNA_ENTITY_PERSON.csv",
      "SNA_ENTITY_CORP.csv",
      "SNA_ENTITY_CAR.csv",
      "SNA_ENTITY_TELPHONE.csv",
      "SNA_ENTITY_ACCOUNT.csv",
      "SNA_ENYITY_EMPLOYEE.csv",
      "SNA_ENTITY_POLICY.csv",
      "SNA_ENTITY_CASE.csv",
      "SNA_ENTITY_GARAGE.csv"
    )
    //此处设计泛型知识，在向List传进数据，他是不记录类型的。只有指定[RDD[(Long, Map[String, String])]]
    //list才会记录做装的数据的类型。假如不指定[RDD[(Long, Map[String, String])]]
    //Main()中在使用List的时候要求他给出所装数据的类型，但是List拿不出，所以报错
    //泛型的定义只是再编译的时候起作用，我们可以规范化数据类型，只要编译通过，运行就不会报错
    //    var entity_RDD = List[RDD[(Long, Map[String, String])]]()
    //
    var entity_RDD = ArrayBuffer[RDD[(Long, Map[String, String])]]()
    for (i <- items) {
      entity_RDD += GraphUtil.toEntity(sc.textFile(vertexesDir + i), i)
    }
    sc.union(entity_RDD)
  }

}
