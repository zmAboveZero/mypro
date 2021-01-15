package Leetcode1

import scala.collection.mutable.ArrayBuffer

object L22GenerateParentheses {


  def main(args: Array[String]): Unit = {
    var re = ArrayBuffer[String]()
    v1("", 0, 0, 1, re)
    ""

  }


  def v1(combination: String, open: Int, close: Int, max: Int, re: ArrayBuffer[String]): Unit = {


    if (combination.length == max * 2) {
      re += (combination)
    }
    if (open < max) {
      v1(combination + "(", open + 1, close, max, re)
    }
    if (close < open) {
      v1(combination + ")", open, close + 1, max, re)
    }

  }

}
