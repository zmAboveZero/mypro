package Leetcode1

object L4MedianofTwoSortedArrays {


  def main(args: Array[String]): Unit = {

    var nums1 = Array(10, 20, 30, 35)
    var nums2 = Array(3, 4, 9, 40)
    v1(nums1, nums2)
  }

  def v1(nums1: Array[Int], nums2: Array[Int]) = {
    var count = 0
    var index1 = 0
    var index2 = 0
    var target_index = (nums1.length + nums2.length) / 2
    //    println(target_index - 1)
    if ((nums1.length + nums2.length) % 2 == 0) {
      while (index1 + index2 + 2 <= target_index) {
        if (nums1(index1) < nums2(index2)) {
          index1 += 1
        } else {
          index2 += 1
        }
      }

      if (nums1(index1) < nums2(index2)) {
        (nums1(index1) + Math.min(nums2(index2), nums1(index1 + 1))) / 2.0
      } else {
        (nums2(index2) + Math.min(nums1(index1), nums2(index2+1))) / 2.0
      }


    } else {
      while (index1 + index2 + 1 <= target_index) {
        if (nums1(index1) < nums2(index2)) {
          index1 += 1
        } else {
          index2 += 1
        }
      }
      //      return Math.min(nums1(index1), nums2(index2))
    }

    println(Math.min(nums1(index1), nums2(index2)))

    //    println(index1)
    //    println(index2)


    //    println(nums1(index1))
    //    println(nums2(index2))
    //    println((nums1(index1) + nums2(index2)) / 2.0)


  }

}
