package com.zm2

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TE {
  def main(args: Array[String]): Unit = {


    //    Array(Array("a", 2), Array("g", 3), Array("e", 5), Array("d", 6)).sortBy(e => e(0))

    val data = Array[Array[Any]](
      Array[Any](1, null, 55, null, 2013, "crms"),
      Array[Any](1, 2, 33, 225, 2016, "abc"),
      Array[Any](1, 3, 53, 776, 2020, "abc"),
      Array[Any](2, 4, 23, 254, 2009, "gda"),
      Array[Any](1, 2, 65, 236, 2017, "aaa"),
      Array[Any](3, 1, 77, 111, 1985, "ffss"),
      Array[Any](1, 2, 43, null, 1960, "aaa")
    )
    val conf: SparkConf = new SparkConf().setAppName("a").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val input = sc.parallelize(data)
    val g = input.groupBy(e => e(0))


    //    var haha = g.map { e =>
    //      var aa: Array[Any] = e._2.toArray.filter(ee => ee(5) == "crms")(0)
    //      var bb = e._2.toArray.filterNot(e => e(5) == "crms").sortBy(e => e(4))
    //      var index = 0
    //      var coll = ArrayBuffer[Int]()
    //      while (index <= 5) {
    //        if (aa(index) == null && bb(0)(index) != null) {
    //          aa(index) = bb(0)(index)
    //        }
    //        if (aa(index) == null) {
    //          coll += index
    //        }
    //        index += 1
    //      }
    //      for (i <- coll) {
    //        var flag = true
    //        for (j <- e._2 if flag) {
    //          if (j(i) != null) {
    //            aa(i) = j(i)
    //            flag = false
    //          }
    //        }
    //      }
    //      aa
    //    }
    //    haha.collect().foreach(println(_))
    //    println("fsfsf")
  }

}
