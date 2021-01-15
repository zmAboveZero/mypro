package leetcode.Leetcode1

import scala.math.{abs, max}
import scala.collection.mutable.ArrayBuffer

object L53MaximumSubarray {


  def main(args: Array[String]): Unit = {


    var nums = Array(-2, 1, -3, 4, -1, 2, 1, -5, 4)
    v2(nums)



  }


  def v2(nums: Array[Int]) = {


  }


  def v1(nums: Array[Int]) = {
    val len: Int = nums.length
    val buffer = ArrayBuffer[Int]()
    for (i <- 1 to len) {
      var index = 0
      while (index + i <= len) {
        val subsum: Int = nums.slice(index, index + i).sum
        buffer += subsum
        index += 1
      }
    }

    max(buffer.max, nums.max)


  }
}
