package leetcode.Leetcode1

object L88MergeSortedArray {

  def main(args: Array[String]): Unit = {
    var nums1 = Array(1,1,1,1, 2, 3, 0, 0, 0, 0, 0, 0)
    var nums2 = Array(2, 5, 6)
    println(v1(nums1, 6, nums2, 3))
    println(nums1.seq)
  }

  def v1(nums1: Array[Int], m: Int, nums2: Array[Int], n: Int): Unit = {

    var num1 = nums1
    var num2 = nums2
    if (num1 == 0) {
      num1 = nums2
      return

    }
    if (n == 0) {
      return
    }


    var index1 = m - 1
    var index2 = n - 1
    var p = nums1.length - 1


    for (i <- 0 until (n)) {
      var flag = true
      for (j <- 0 until (m) if flag) {
        //        println(index2 - i, "==========", index1 - j)


        if (nums2(index2 - i) > nums1(index1 - j)) {
          nums1(p) = nums2(index2 - i)
          p -= 1
          flag = false
        } else {
          nums1(p) = nums1(index1 - j)
          p -= 1
        }


      }
    }

    return nums1.slice(num1.length - m - n, num1.length)
  }
}
