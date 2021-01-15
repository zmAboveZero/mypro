package Leetcode1

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object L55JumpGame {
  def main(args: Array[String]): Unit = {

    var nums = Array(2, 3, 1, 1, 4)

    val ints: mutable.MutableList[Int] = mutable.MutableList[Int]()
    val int3: mutable.MutableList[Int] = ints += (5)
    println((ints += (5)) == ints)


    var index = 0
    while (index <= nums.length - 1) {
      index += nums(index)
    }
    println(index == nums.length - 1)


  }

}
