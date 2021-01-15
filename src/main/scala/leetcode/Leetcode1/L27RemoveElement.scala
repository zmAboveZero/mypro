package leetcode.Leetcode1

import java.util

object L27RemoveElement {


  def main(args: Array[String]): Unit = {

    //    [3,2,2,3]
    //    3
    var nums = Array[Int](3, 2, 2, 3)
    var nums1 = Array[String]("99", "AA", "BB", "CC")
    //    println(nums.indexOf(2))
    //    var he = nums.drop(1)


    val strings: Array[String] = nums1.slice(1, 3)
    strings(0) = "ddddddddddddddddd"
    println(strings.toBuffer)
    println(nums1.toBuffer)

    //    println(he.length)
    //    println(he.toBuffer)
    //    println(nums1(0))
    //    v1(nums, 3)


  }

  def v1(nums: Array[Int], `val`: Int) = {

    var index = 0
    for (e <- 0 until nums.length) {

      if (nums(e) == `val`) {
        println(e)

      }


    }


  }

}
