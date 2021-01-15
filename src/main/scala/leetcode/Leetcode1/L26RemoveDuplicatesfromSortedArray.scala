package leetcode.Leetcode1

object L26RemoveDuplicatesfromSortedArray {


  def main(args: Array[String]): Unit = {


    var nums = Array[Int]()


    v1(nums)
  }


  def v2(nums: Array[Int]): Int = {
    var index = 0
    while (index <= nums.length) {
      if (nums(index) != nums(index - 1)) {

        index += 1
      }

    }
    index

  }


  def v1(nums: Array[Int]): Int = {

    if (nums.isEmpty) {
      return 0
    }
    var index = 0


    for (e <- 1 until nums.length) {
      if (nums(e) != nums(e - 1)) {
        index += 1
        nums(index) = nums(e)
      }
    }


    index + 1


  }


}
