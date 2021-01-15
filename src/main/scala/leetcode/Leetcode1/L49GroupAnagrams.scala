package leetcode.Leetcode1

import scala.collection.mutable.ListBuffer

object L49GroupAnagrams {
  def main(args: Array[String]): Unit = {
    val strs = Array("eat", "tea", "tan", "ate", "nat", "bat")
    var map = Map[Int, String]()
    for (i <- strs.indices) {
      map += (i -> strs(i).sorted)
    }

    var u = List[Int]()
    //    u += (55)


    var re = List[List[String]]()


    for (i <- map.groupBy(e => e._2).values) {
      var list = List[String]()
      for (j <- i.keySet) {
        list :+= (strs(j))
      }
      re :+= list
    }

    println(re)


  }


}
