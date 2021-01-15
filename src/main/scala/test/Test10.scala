package test

import scala.collection.mutable.ListBuffer

object Test10 {

  def main(args: Array[String]): Unit = {
    var list = List[Int](13, 3, 4, 5, 6)
    var hello = list
    hello :+= (22222)
    println(list)

    val buffer = ListBuffer(2, 2, 3, 43, 4)
    var wold = buffer
    wold(0) = (555)
    println(buffer == wold)


  }

  def v1(list: List[Int]) = {
    var list1 = list
  }


}
