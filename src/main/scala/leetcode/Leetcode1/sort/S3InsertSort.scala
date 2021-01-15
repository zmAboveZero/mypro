package leetcode.Leetcode1.sort

object S3InsertSort {
  def main(args: Array[String]): Unit = {

    var nums = Array(8, 2, 5, 9, 7)
    v1(nums)
  }

  def v1(nums: Array[Int]) = {
    for (i <- nums.indices) {
      var flag = true
      var temp = nums(i)
      var ind = 0
      for (j <- 1 to (i) if flag) {
        if (temp < nums(i - j)) {
          nums(i - j + 1) = nums(i - j)
        } else {
          ind = i - j + 1
          flag = false
        }
      }
      println(ind)
      nums(ind) = temp
      println(nums.seq)
      println("================")
    }

  }
}
