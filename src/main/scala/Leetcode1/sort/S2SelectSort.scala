package Leetcode1.sort

object S2SelectSort {
  def main(args: Array[String]): Unit = {
    var nums = Array(8, 2, 5, 9, 7)
    v1(nums)
  }

  def v1(nums: Array[Int]) = {
    for (i <- nums.indices) {
      var min = i
      for (j <- nums.indices.drop(i)) {
        if (nums(j) < nums(min)) {
          min = j
        }
      }
      var temp = nums(min)
      nums(min) = nums(i)
      nums(i) = temp
    }
    println(nums.seq)
  }


}
