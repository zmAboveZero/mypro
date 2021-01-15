package leetcode.Leetcode1.DP

object D2KingAndGold {

  def main(args: Array[String]): Unit = {


    var gold = Array(400, 500, 200, 300, 350)
    var persons = Array(5, 5, 3, 4, 3)
    println(v2(5, 10, gold, persons))
  }

  def v2(remainM: Int, remainP: Int, values: Array[Int], workers: Array[Int]): Int = {
    var col = remainP + 1
    var preResult = new Array[Int](col)
    var result = new Array[Int](col)
    for (i <- 0 to remainP) {
      if (i < workers(0)) {
        preResult(i) = 0
      } else {
        preResult(i) = values(0)
      }
    }

    println("=========",preResult.seq)
    for (i <- 0 until remainM) {
      for (j <- 0 until (col)) {
        if (j < workers(i)) {
          result(j) = preResult(j)
        } else {
          result(j) = Math.max(preResult(j), preResult(j - workers(i)) + values(i))
        }
      }
      for (j <- 0 until (col)) {
        preResult(j) = result(j)
      }

    }
    return result(remainP)
  }

  def v1(remainM: Int, remainP: Int, values: Array[Int], workers: Array[Int]): Int = {

    if (remainM <= 1 && remainP < workers(0)) {
      return 0
    }
    if (remainM == 1 && remainP >= workers(0)) {
      return values(0)
    }
    if (remainM > 1 && remainP < workers(remainM - 1)) {
      return v1(remainM - 1, remainP, values, workers)
    }
    var a = v1(remainM - 1, remainP, values, workers)
    var b = v1(remainM - 1, remainP - workers(remainM - 1), values, workers) + values(remainM - 1)
    return Math.max(a, b)
  }

}
