package Leetcode1

import scala.collection.mutable.ArrayBuffer

object L14LongestCommonPrefix {


  def main(args: Array[String]): Unit = {
    //    var strs = Array[String]("alower", "blow", "vlight")
    //    var strs = Array[String]()
    var strs = Array[String]("c", "acc", "ccc")


    if (strs.length == 0) return ""

    println("hello"(0))
    for (i <- 1 until 10; j <- 1 to 3) {
      println(j)
    }


    val buffer = ArrayBuffer[Char]()


    var flag = true

    for (i <- 0 until strs(0).length if flag) {
      val st: Char = strs(0).charAt(i)
      for (j <- 1 until (strs.length)) {
        try {
          if (strs(j).charAt(i) != st)
            flag = false
        }
        catch {
          case e: Exception => return buffer.mkString
        }

      }
      if (flag) {
        buffer += (st)
      }
    }
    println(buffer.mkString)


  }

}
