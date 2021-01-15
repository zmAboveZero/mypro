package Leetcode1.DP

object D1Fibonacci {
  var counter = 0

  def main(args: Array[String]): Unit = {
    var n = 6
    println(v1(n))
    println(v2(n))
  }

  def v1(n: Int): Int = {
    counter += 1
    if (n == 0) {
      return 0
    }
    if (n == 1) {
      return 1
    }
    return v1(n - 1) + v1(n - 2)
  }

  def v2(n: Int): Int = {
    if (n == 0) {
      return 0
    } else if (n == 1 || n == 2) {
      return 1
    } else {
      val memo = Array.fill(n + 1)(-1)
      memo(0) = 0
      memo(1) = 1
      memo(2) = 1
      for (i <- 3 to n) {
        if (memo(i) == -1) {
          memo(i) = memo(i - 1) + memo(i - 2)
        }
      }
      return memo(n)
    }

  }
}
