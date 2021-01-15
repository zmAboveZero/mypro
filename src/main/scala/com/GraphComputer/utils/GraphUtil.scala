package com.GraphComputer.utils

import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object GraphUtil {
  def toRel(input: RDD[String], label: String) = {
    input.map(e => {
      val items = e.split(",")
      Edge(items(1).toLong, items(2).toLong, label)
    }
    )
  }

  def toEntity[A: ClassTag](input: RDD[String], label: String) = {
    label match {
      case "SNA_ENTITY_PERSON.csv" => input.map(e => {
        val items = e.split(",")
        (items(0).toLong, Map("label" -> "person"))
      }
      )
      case "SNA_ENTITY_CORP.csv" => input.map(e => {
        val items = e.split(",")
        (items(0).toLong, Map("label" -> "cop"))
      }
      )
      case "SNA_ENTITY_CAR.csv" => input.map(e => {
        val items = e.split(",")
        (items(0).toLong, Map("label" -> "car"))
      }
      )
      case "SNA_ENTITY_TELPHONE.csv" => input.map(e => {
        val items = e.split(",")
        (items(0).toLong, Map("label" -> "telphone"))
      }
      )
      case "SNA_ENTITY_ACCOUNT.csv" => input.map(e => {
        val items = e.split(",")
        (items(0).toLong, Map("label" -> "account"))
      }
      )
      case "SNA_ENYITY_EMPLOYEE.csv" => input.map(e => {
        val items = e.split(",")
        (items(0).toLong, Map("label" -> "employee"))
      }
      )
      case "SNA_ENTITY_POLICY.csv" => input.map(e => {
        val items = e.split(",")
        (items(0).toLong, Map("label" -> "policy"))
      }
      )
      case "SNA_ENTITY_CASE.csv" => input.map(e => {
        val items = e.split(",")
        (items(0).toLong, Map("label" -> "case"))
      }
      )
      case "SNA_ENTITY_GARAGE.csv" => input.map(e => {
        val items = e.split(",")
        (items(0).toLong, Map("label" -> "garage"))
      }
      )
    }
  }
}
