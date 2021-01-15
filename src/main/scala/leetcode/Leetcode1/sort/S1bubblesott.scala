package leetcode.Leetcode1.sort

object S1bubblesott {

  def main(args: Array[String]): Unit = {
    var nums = Array(8, 2, 5, 9, 7)
    v3(nums)
  }

  def v3(nums: Array[Int]) = {
    for (i <- nums.indices) {
      for (j <- nums.indices.dropRight(i + 1)) {
        if (nums(j) > nums(j + 1)) {
          var temp = nums(j)
          nums(j) = nums(j + 1)
          nums(j + 1) = temp
        }
      }
    }
    println(nums.seq)
  }

  def v2(nums: Array[Int]) = {
    for (i <- nums.indices) {
      for (j <- 0 until nums.length - i - 1) {
        if (nums(j) > nums(j + 1)) {
          var temp = nums(j)
          nums(j) = nums(j + 1)
          nums(j + 1) = temp
        }
      }
    }
    println(nums.seq)
  }

  def v1(nums: Array[Int]) = {
    var index = 0
    for (i <- nums.indices) {
      while (index < nums.length - i - 1) {
        if (nums(index) > nums(index + 1)) {
          var temp = nums(index)
          nums(index) = nums(index + 1)
          nums(index + 1) = temp
        }
        index += 1
      }
    }

    println(nums.seq)


  }
}
