package com.socialgraphjob

import org.apache.spark.graphx.Edge

import scala.collection.mutable.ListBuffer

object InputDataFlow {
  def makeEdges(line:String) ={
    var edges=new ListBuffer[Edge[Int]]()
    val fields: Array[String] = line.split(" ")
    val orignal: String = fields(0)
    (1 until fields.length).foreach{
      p=>edges+=Edge(orignal.toLong,fields(p).toLong,0)
    }
    edges.toList

  }

  def parseNames(line:String) = {
    val fields: Array[String] = line.split('\t')
    if (fields.length>1) {
      Some(fields(0).trim.toLong,fields(1))
    }
    else  None
  }


}
