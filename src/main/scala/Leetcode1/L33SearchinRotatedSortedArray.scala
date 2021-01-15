package Leetcode1

object L33SearchinRotatedSortedArray {

  def main(args: Array[String]): Unit = {
    val nums = Array(4, 5, 6, 7, 0, 1, 2)
    println(v2(nums, 0))
  }

  def v1(nums: Array[Int], target: Int): Int = {
    var flag = true
    for (i <- nums.indices if flag) {
      if (nums(i) == target) {
        return i
      }
    }
    return -1
  }


  def v2(nums: Array[Int], target: Int): Int = {
    var index = 0
    var len = nums.length - 1
    while (index <= len) {
      if (nums(index) == target) {
        return index
      }
      index += 1
    }
    return -1

  }


}
