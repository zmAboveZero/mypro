package leetcode.Leetcode1.sort

object S4MergeSort {
  def main(args: Array[String]): Unit = {
    var nums = Array(8, 2, 5, 9, 7)
    v1(nums)
    println(nums.seq)
  }

  def v1(nums: Array[Int]) = {
    var tempArr = new Array[Int](nums.length)
    sort(nums, tempArr, 0, nums.length - 1)
  }

  def sort(nums: Array[Int], tempArr: Array[Int], startIndex: Int, endIndex: Int): Unit = {
    if (endIndex <= startIndex) {
      return
    }
    var midIndex = (startIndex + endIndex) / 2
    sort(nums, tempArr, startIndex, midIndex)
    sort(nums, tempArr, midIndex + 1, endIndex)
    merge(nums, tempArr, startIndex, midIndex, endIndex)
  }

  def merge(nums: Array[Int], tempArr: Array[Int], startIndex: Int, midIndex: Int, endIndex: Int) = {
    for (i <- startIndex to endIndex) {
      var a = i
      tempArr(i) = nums(i)
    }
    var left = startIndex
    var right = midIndex + 1
    for (i <- startIndex to endIndex) {
      if (left > midIndex) {
        nums(i) = tempArr(right)
        right += 1
      } else if (right > endIndex) {
        nums(i) = tempArr(left)
        left += 1
      } else if (tempArr(right) < tempArr(left)) {
        nums(i) = tempArr(right)
        right += 1
      } else {
        nums(i) = tempArr(left)
        left += 1
      }
    }
  }


}
