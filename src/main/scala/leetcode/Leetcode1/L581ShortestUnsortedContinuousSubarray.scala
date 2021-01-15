package leetcode.Leetcode1

object L581ShortestUnsortedContinuousSubarray {

  def main(args: Array[String]): Unit = {
    //    var nums = Array(1, 2, 3, 4, 5, 6, 8, 9, 7, 6, 10, 5, 11, 12)
    var nums = Array(2, 6, 4, 8, 10, 9, 15)
    println(v3(nums))
  }

  def v3(nums: Array[Int]): Int = {

    var len = nums.length
    var max = nums(0)
    var min = nums(len - 1)
    var l = 0
    var r = -1
    for (i <- nums.indices) {
      if (max > nums(i)) {
        r = i
      } else {
        max = nums(i)
      }
      if (min < nums(len - 1 - i)) {
        l = len - 1 - i
      } else {
        min = nums(len - 1 - i)
      }
    }
    return r - l + 1
  }

  def v2(nums: Array[Int]): Int = {
    var index1 = 0
    var index2 = nums.length - 1
    while (index1 < nums.length - 1 && nums(index1) <= nums(index1 + 1)) {
      index1 += 1
    }
    if (index1 == nums.length - 1) {
      return nums.length
    }
    while (nums(index2) > nums(index2 - 1)) {
      index2 -= 1
    }
    var max = Int.MinValue
    var min = Int.MaxValue
    while (index1 <= index2) {
      if (nums(index1) < min) {
        min = nums(index1)
      }
      if (nums(index1) > max) {
        max = nums(index1)
      }
      index1 += 1
    }
    index1 = 0
    index2 = nums.length - 1
    while (nums(index1) <= min) {
      index1 += 1
    }
    while (nums(index2) >= max) {
      index2 -= 1
    }
    index2 - index1 + 1
  }

  def v1(nums: Array[Int]) = {

    var index = 0
    var max = 0
    var tempmin = Int.MaxValue
    var start = 0
    var end = 0

    //    while (index <= nums.length - 1) {
    //
    //
    //      if (nums(index) < nums(index + 1)) {
    //
    //        max = nums(index)
    //
    //        while (max > nums(index)) {
    //          if (nums(index) < tempmin) {
    //            tempmin = nums(index)
    //          }
    //          index += 1
    //        }
    //
    //        while (nums(start) > tempmin) {
    //          start -= 1
    //        }
    //
    //
    //      }
    //
    //
    //      index += 1
    //    }


    while (nums(index) < nums(index + 1)) {
      index += 1
    }
    start = index
    max = nums(index)
    tempmin = nums(index)
    println(nums(index))
    while (nums(index) <= max) {
      if (nums(index) < tempmin) {
        tempmin = nums(index)
      }
      index += 1
    }
    println(tempmin)
    while (nums(start) > tempmin) {
      start -= 1
    }
    println(index)

  }

}
