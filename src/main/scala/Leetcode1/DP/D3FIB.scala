package Leetcode1.DP

object D3FIB {


  def main(args: Array[String]): Unit = {
    println(v3(10))

  }


  def v3(n: Int): Int = {
    val arr = new Array[Int](n + 1)
    arr(1) = 1
    arr(2) = 1
    for (i <- 2 to n) {
      arr(i) = arr(i - 1) + arr(i - 2)
    }
    return arr(n)
  }


  def v2(n: Int, memo: Array[Int]): Int = {
    if (n <= 2) {
      memo(n) = 1
    }
    if (memo(n) != -1) {
      return memo(n)
    }
    memo(n) = v2(n - 1, memo) + v2(n - 2, memo)
    return memo(n)
  }

  def v1(n: Int): Int = {
    if (n == 0) {
      return 0
    }
    if (n == 1) {
      return 1
    }
    var fib = v1(n - 1) + v1(n - 2)
    return fib
  }


}
