package leetcode.Leetcode1

object L283MoveZeroes {

  def main(args: Array[String]): Unit = {
    var nums = Array(2)
    v3(nums)
    println(nums.seq)
  }

  def v3(nums: Array[Int]): Unit = {

    if (nums.length == 1) {
      return nums
    }
    var index1 = 0
    var index2 = 0
    var len = nums.length - 1

    while (index1 <= len) {
      if (nums(index1) != 0) {
        var c = nums(index2)
        nums(index2) = nums(index1)
        nums(index1) = c
        index2 += 1
      }
      index1 += 1
    }
  }

  def v2(nums: Array[Int]): Unit = {

    if (nums.length == 1) {
      return nums
    }
    var index1 = 0
    var index2 = 0
    var len = nums.length - 1
    if (nums(0) != 0) {
      while (nums(index1) != 0) {
        index1 += 1
        index2 += 1
        if (index1 == len) {
          return nums
        }
      }
    }

    //    if (index1 == len) return nums
    while (index1 <= len) {
      if (nums(index1) != 0) {
        nums(index2) = nums(index1)
        nums(index1) = 0
        index2 += 1
      }
      index1 += 1
    }


  }

  def v1(nums: Array[Int]): Unit = {

    if (nums.length == 1 || !nums.contains(0)) {
      return nums

    }
    var index1 = 0
    var index2 = 0
    var len = nums.length - 1
    if (nums(0) != 0) {
      while (nums(index1) != 0) {
        index1 += 1
        index2 += 1
      }
      while (index1 <= len) {
        if (nums(index1) != 0) {
          nums(index2) = nums(index1)
          nums(index1) = 0
          index2 += 1
        }
        index1 += 1
      }
    } else {
      while (index1 <= len) {
        if (nums(index1) != 0) {
          nums(index2) = nums(index1)
          nums(index1) = 0
          index2 += 1
        }
        index1 += 1
      }
    }

  }


}
