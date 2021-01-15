package Leetcode1

object L02ReverseIntegerV2 {

  def main(args: Array[String]): Unit = {

    var x = -123589

    var temp = 0
    var ret = 0



    while (x != 0) {
      temp = ret * 10 + x % 10
      ret = temp

      if (temp / 10 != ret)
        return 0;
      x /= 10
    }
    temp
    println(temp)


  }

}
