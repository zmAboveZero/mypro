package com.connectcomponent

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.CommonUtils

object ConnectCom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Connected Components Test").setMaster("local[1]")
    val sc = new SparkContext(conf)


    case class Person(name: String, age: Int)
    val people = sc.textFile(CommonUtils.getResourcesPath() + "people.csv")
    val edge = sc.textFile(CommonUtils.getResourcesPath() + "edge.csv")

    println(people.count())
    println(edge.count())
    //    val peopleRDD: RDD[(VertexId, Person)] = people.map(line => line.split(","))
    //      .map(row => (row(0).toInt, Person(row(1), row(2).toInt)))
    //
    //    val links = sc.textFile("src/main/resources/links.csv")
    //    type Connection = String
    //    val linksRDD: RDD[Edge[Connection]] = links.map({ line =>
    //      val row = line.split(",")
    //      Edge(row(0).toInt, row(1).toInt, row(2))
    //    })
    //
    //    val defaultPeople = Person("Missing", -1)
    //    val tinySocial: Graph[Person, Connection] = Graph(peopleRDD, linksRDD, defaultPeople)
    //
    //    val cc = tinySocial.connectedComponents()
    //
    //    println(cc.vertices.collect().toList)
    //    sc.stop()
  }

}
