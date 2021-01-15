package Leetcode2

object L997FindtheTownJudge {

  def main(args: Array[String]): Unit = {


    val trust = Array(Array(1, 3), Array(1, 4), Array(2, 3), Array(2, 4), Array(4, 3))

    v1(4, trust)


  }

  def v1(i: Int, trust: Array[Array[Int]]) = {
    val intToInt: Map[Int, Int] = trust.map(ele => Array(ele(1), 1)).groupBy(ele => ele(0))
      .map(e => {
        val value: Array[Array[Int]] = e._2
        val suma: Int = value.map(a => a(1)).sum
        (e._1, suma)
      })
    var re = intToInt.toArray.maxBy(_._2)
    println(re)
    println(intToInt)
    trust.isEmpty

    //    if (re._2 != N - 1||intToInt.contains(re._2)) {
    //      return -1
    //    }
    //    re._1

  }

}
