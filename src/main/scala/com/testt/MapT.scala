package com.testt

import scala.collection.mutable

object MapT {
  def main(args: Array[String]): Unit = {
    var map = mutable.Map[Long, Long]()
    var maps = Map(1 -> 2, 3 -> 10, 3 -> 5)
    println(maps)
    map.put(2, 2)
    map.put(2, 9)
    println(map ++ maps)
    var s = map.keySet ++ maps.keySet
    println(s)
  }

}
