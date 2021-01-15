package leetcode.Leetcode1

import scala.collection.mutable.ArrayBuffer

object L39CombinationSum {

  def main(args: Array[String]): Unit = {
    var candidates = Array(2, 3, 6, 7)
    var target = 7

    v1(candidates, target)
  }

  def v1(candidates: Array[Int], target: Int): ArrayBuffer[List[Int]] = {

    var re = ArrayBuffer[List[Int]]()
    //    if () {
    //
    //    }
    //
    //
    //    for (i <- candidates) {
    //
    //      v1(candidates, target - i)
    //
    //    }

    v2(candidates, target, List[Int](), re)
    re
  }

  def v2(candidates: Array[Int], target: Int, list: List[Int], re: ArrayBuffer[List[Int]]): Unit = {
    if (candidates.contains(target)) {
      re += list
    }
    for (i <- candidates.indices) {
      v2(candidates, target - candidates(i), list :+ candidates(i), re)
    }
  }
}