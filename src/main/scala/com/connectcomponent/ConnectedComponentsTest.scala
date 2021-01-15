package com.connectcomponent

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ConnectedComponentsTest {

  def main(args: Array[String]): Unit = {
    Logger getLogger ("org") setLevel (Level.ERROR)
    val conf = new SparkConf().setAppName("Connected Components Test").setMaster("local[1]")
    val sc = new SparkContext(conf)

    case class Person(name: String, age: Int)
    val people = sc.textFile("src/main/resources/people.csv")
    val peopleRDD: RDD[(VertexId, Person)] = people.map(line => line.split(","))
      .map( row => (row(0).toInt, Person(row(1), row(2).toInt)))

    val links = sc.textFile("src/main/resources/links.csv")
    type Connection = String
    val linksRDD: RDD[Edge[Connection]] = links.map({line =>
      val row = line.split(",")
      Edge(row(0).toInt, row(1).toInt, row(2))
    })

    val defaultPeople = Person("Missing",-1)
    val tinySocial: Graph[Person, Connection] = Graph(peopleRDD, linksRDD, defaultPeople)
    val cc = tinySocial.connectedComponents()
    val newGraph = cc.outerJoinVertices(peopleRDD)((id, cc, p)=>(cc,p.get.name,p.get.age))
    cc.vertices.take(10).foreach(println(_))
    println("--------------------------")
    newGraph.vertices.take(10).foreach(println(_))
    cc.vertices.map(_._2).collect.distinct.foreach(id =>{
      val sub = newGraph.subgraph(vpred = (id1, id2) => id2._1==id)
      println(sub.triplets.collect().mkString(","))
      println("===")
    })
    sc.stop()
  }
}
