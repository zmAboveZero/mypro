package Leetcode1.DP

object D5IronCut {

  def main(args: Array[String]): Unit = {
    var price = Array(1, 5, 8, 9, 17, 17, 20, 24, 30)
    println(v1(price, 4))
  }

  def v1(price: Array[Int], n: Int): Int = {
    if (n == 0) {
      return 0
    }
    var q = 0
    for (i <- 1 to (n)) {
      q = Math.max(v1(price, i-1) + price(i - 1), q)
    }
    q
  }

}
