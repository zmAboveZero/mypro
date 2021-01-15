package Leetcode1

object L35SearchInsertPosition {


  def main(args: Array[String]): Unit = {
    var nums = Array(1, 4, 6, 7, 8)
    var target = 6
    println(v4(nums, target))
  }

  def v4(nums: Array[Int], target: Int): Int = {
    if (target > nums.last) {
      return nums.length
    }
    if (target <= nums.head) {
      return 0
    }
    var index = 0
    var len = nums.length - 1
    while (nums(index) > target || nums(index + 1) < target) {
      if (nums(index) < target) {
        index = (index + len) / 2
      }
      if (nums(index) > target) {
        index = index / 2
      }
    }


    return index + 1


  }

  def v2(nums: Array[Int], target: Int): Int = {
    if (target > nums.last) {
      return nums.length
    }
    if (target <= nums.head) {
      return 0
    }
    v3(nums, target, 0, nums.length - 1)
  }

  def v3(nums: Array[Int], target: Int, start: Int, end: Int): Int = {
    var mid = (start + end) / 2
    if (nums(mid) <= target && target <= nums(mid + 1)) {
      return mid
    }
    if (nums(mid) > target) {
      v3(nums, target, 0, mid)
    }
    if (nums(mid) < target) {
      v3(nums, target, 0, mid)
    }

    return 0
  }

  def v1(nums: Array[Int], target: Int): Int = {
    for (i <- 0 until (nums.length)) {
      if (nums(i) >= target) {
        return i
      }
      if (i == nums.length - 1) {
        return nums.length
      }
    }
    return -1


  }

}
