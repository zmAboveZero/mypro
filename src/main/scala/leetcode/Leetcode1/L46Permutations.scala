package leetcode.Leetcode1

import scala.collection.mutable.ArrayBuffer

object L46Permutations {

  def main(args: Array[String]): Unit = {
    var nums = Array(1, 2, 3)
    println(v1(nums))
  }

  def v1(nums: Array[Int]): List[List[Int]] = {
    var tar = ArrayBuffer[List[Int]]()
    v2(tar, nums, List[Int]())
    tar.toList
  }

  def v2(tar: ArrayBuffer[List[Int]], nums: Array[Int], list: List[Int]): Unit = {
    if (nums.size == 0) {
      tar += list
      return
    }
    for (i <- nums.indices) {
      v2(tar, nums.filter(e => e != nums(i)), list :+ nums(i))
    }
  }
}
