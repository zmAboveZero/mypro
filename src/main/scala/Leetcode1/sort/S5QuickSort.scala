package Leetcode1.sort

object S5QuickSort {

  def main(args: Array[String]): Unit = {
    var nums = Array(10, 7, 2, 4, 7, 62, 3, 4, 2, 1, 8, 9, 19)
    v1(nums)
    println(nums.seq)
  }

  def v1(nums: Array[Int]) = {
    quickSort(nums, 0, nums.length - 1)
  }


  def quickSort(nums: Array[Int], start: Int, end: Int): Unit = {
    var i, j, temp, t = 0
    if (start > end) {
      return
    }
    i = start
    j = end
    temp = nums(start)
    while (i < j) {
      while (temp <= nums(j) && i < j) {
        j -= 1
      }
      while (temp >= nums(i) && i < j) {
        i += 1
      }
      if (i < j) {
        t = nums(j)
        nums(j) = nums(i)
        nums(i) = t
      }
    }
    nums(start) = nums(i)
    nums(i) = temp
    quickSort(nums, start, j - 1)
    quickSort(nums, j + 1, end)
  }

}
