package Leetcode1

import scala.math.{abs}

object L02ReverseInteger {
  def main(args: Array[String]): Unit = {

    var x = -2147483648
    if (x > Int.MaxValue || x <= Int.MinValue) {
      return 0

    }
    val array: Array[Char] = abs(x).toString.toCharArray
    var start = 0
    var end = array.length - 1


    while (start < end) {
      val temp = array(start)
      array(start) = array(end)
      array(end) = temp
      start += 1
      end -= 1
    }
    var re: Long = array.mkString.toLong

    if (x < 0) {
      re = -re
    }

    if (re > Int.MaxValue || re <= Int.MinValue) {
      re = 0
    }
    re.toInt
    println(Int.MaxValue)
    println(Int.MinValue)
    println(re)


    //    var dd=x.toString.toCharArray
    //    println(dd(0))

    //    var input = x.toString.toCharArray
    //    //    println(input.length)
    //    var tem = new Array[Char](1)
    //    var re = new Array[Char](input.length)
    //
    //    if (input(0) + "" == "-") {
    //      re(0) = "-".charAt(0)
    //      for (i <- 2 until (input.length + 1)) {
    //        //        println(i)
    //        re(i - 1) = input(input.length + 1 - i)
    //      }
    //    } else {
    //      for (i <- 1 until (input.length + 1)) {
    //        re(i - 1) = input(input.length - i)
    //      }
    //    }
    //
    //    println(re.mkString)

  }
}
