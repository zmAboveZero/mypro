package Leetcode2

import java.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object L1042FlowerPlantingWithNoAdjacent {

  def main(args: Array[String]): Unit = {
    //    val paths = Array(Array(1, 2), Array(3,4))
    val paths = Array(Array(7, 4), Array(3, 7), Array(1, 5), Array(5, 4), Array(7, 1), Array(3, 1), Array(4, 3), Array(6, 5))
    var N = 8
    v1(N, paths)
  }

  def v1(N: Int, paths: Array[Array[Int]]) = {
    val answer = ArrayBuffer[Int]()
    answer += 1
    for (index <- 1 until (N)) {
      var avail = mutable.ListBuffer[Int](1, 2, 3, 4)
      for (path <- paths) {
        //        if (index + 1 == path(0) && index + 1 < path(1) || index + 1 == path(1) && index + 1 < path(0)) {
        //          if (avail.contains(answer(path.min - 1))) {
        //            avail.remove(avail.indexOf(answer(path.min - 1)))
        //          }
        //        }
        //        if ((index + 1 == path(0) && index + 1 < path(1)) && avail.contains(answer(path.min - 1))) {
        //
        //        }

        if (path(0) > path(1) && path(0) == index + 1 && avail.contains(answer(path(1) - 1))) {
          avail.remove(avail.indexOf(answer(path(1) - 1)))
        }
        if (path(0) < path(1) && path(1) == index + 1 && avail.contains(answer(path(0) - 1))) {
          avail.remove(avail.indexOf(answer(path(0) - 1)))
        }
      }
      answer += avail.head
    }
    answer.toArray

    println(answer)
  }

}
