package Leetcode1.DP

object D4LongestIncreasingSubsequence {

  def main(args: Array[String]): Unit = {
    var nums = Array(1, 7, 2, 8, 3, 4)
    println(v1(nums))
  }

  def v1(nums: Array[Int]): Int = {
    var dp = new Array[Int](nums.length)
    var maxLen = 1
    for (i <- nums.indices) {
      dp(i) = 1
      for (j <- 0 until (i)) {
        if (nums(i) > nums(j) && dp(i) < (dp(j) + 1)) {
          dp(i) = dp(j) + 1
          maxLen = Math.max(maxLen, dp(i))
        }

      }
    }
    return maxLen
  }
}
