package Leetcode1

import scala.collection.mutable.ArrayBuffer


object L17LetterCombinationsofaPhoneNumber {
  def main(args: Array[String]): Unit = {
    println(letterCombinations("56"))
  }

  var phone = Map(2 -> "abc",
    3 -> "def",
    4 -> "ghi",
    5 -> "jkl",
    6 -> "mno",
    7 -> "pqrs",
    8 -> "tuv",
    9 -> "wxyz")
  var list = ArrayBuffer[String]()

  def letterCombinations(digits: String): List[String] = {
    if (digits.length() != 0) {
      backtrack("", digits)
    }
    return list.toList
  }

  def backtrack(combination: String, next_digits: String): Unit = {
    if (next_digits.length == 0) {
      list += (combination)
    } else {
      var digit = next_digits.head.toString.toInt
      val letters: String = phone(digit)
      for (i <- 0 until (letters.length)) {
        //        println(letters)
        val letter: String = phone(digit).substring(i, i + 1)
        backtrack(combination + letter, next_digits.substring(1))
      }
    }
  }


}
