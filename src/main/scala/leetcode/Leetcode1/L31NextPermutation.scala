package leetcode.Leetcode1

object L31NextPermutation {

  def main(args: Array[String]): Unit = {
    //    val nums = Array(3, 5, 8, 7, 6, 4)
    //    val nums = Array(3, 4, 6, 9, 8, 7, 5, 2, 1)
    val nums = Array(3, 3)
    //    val nums = Array(3, 4, 6, 9, 8, 7, 5, 2, 1)
    v1(nums)
    println(nums.seq)
  }

  def v1(nums: Array[Int]): Unit = {
    if (nums.length == 1) return nums
    var index1 = nums.length - 1
    var index2 = nums.length - 1
    if (nums(index1 - 1) < nums(index1)) {
      var t = nums(index1)
      nums(index1) = nums(index1 - 1)
      nums(index1 - 1) = t
      return
    }
    while (index1 != 0 && nums(index1 - 1) >= nums(index1)) {
      index1 -= 1
    }
    if (index1 == 0) {
      var tem = nums.sorted
      for (i <- tem.indices) {
        nums(i) = tem(i)
      }
      return
    }
    while (nums(index1 - 1) >= nums(index2)) {
      index2 -= 1
    }
    var temp = nums(index1 - 1)
    nums(index1 - 1) = nums(index2)
    nums(index2) = temp
    val tuple: (Array[Int], Array[Int]) = nums.splitAt(index1)
    var re = (tuple._1 ++ tuple._2.sorted)
    for (i <- re.indices) {
      nums(i) = re(i)
    }
  }

}
