package Leetcode1

object L66PlusOne {


  def main(args: Array[String]): Unit = {
    var digits = Array(1, 9, 9)
    println(v1(digits).seq)
  }

  def v1(digits: Array[Int]): Array[Int] = {
    var end = digits.length - 1
    if (digits.last != 9) {
      digits(end) += 1
      return digits
    }
    while (end >= 0 && digits(end) == 9) {
      digits(end) = 0
      end -= 1
    }
    if (end == -1) {
      val ints = new Array[Int](digits.length + 1)
      ints(0) = 1
      return ints
    } else {
      digits(end) += 1
    }
    return digits


  }


}
