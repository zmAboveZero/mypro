package leetcode.Leetcode1

import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._

object L25ThreeSum {

  def main(args: Array[String]): Unit = {
    var nums = Array(-1, 0, 1, 2, -1, -4)
    v1(nums)
  }

  def v1(nums: Array[Int]): List[List[Int]] = {
    var buffer = ArrayBuffer[List[Int]]()
    if (nums == null || nums.length < 3) return buffer.toList
    val sorted: Array[Int] = nums.sorted
    for (i <- sorted.indices) {
      if (sorted(i) > 0) return buffer.toList
      breakable {
        if (i > 0 && sorted(i) == sorted(i - 1)) {
          break()
        } else {
          var left = i + 1
          var right = nums.length - 1
          while (left < right) {
            var sum = sorted(i) + sorted(left) + sorted(right)
            if (sum == 0) {
              buffer += (List(sorted(i), sorted(left), sorted(right)))
              while (left < right && sorted(left) == sorted(left + 1)) {
                left += 1
              }
              while (left < right && sorted(right) == sorted(right - 1)) {
                right -= 1
              }
              left += 1
              right -= 1
            } else if (sum < 0) {
              left += 1
            } else if (sum > 0) {
              right -= 1
            }
          }
        }
      }
    }
    buffer.toList

  }


}
