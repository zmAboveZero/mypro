package Leetcode1

object L11ContainerWithMostWater {

  def main(args: Array[String]): Unit = {
    var height = Array(1, 8, 6, 2, 5, 4, 8, 3, 7)

    println(v1(height))


  }

  def v1(height: Array[Int]) = {
    var maxValue = 0
    var l = 0
    var r = height.length - 1
    while (l < r) {
      maxValue = Math.max(Math.min(height(l), height(r)) * (r - l), maxValue)
      if (height(l) < height(r)) {
        l += 1
      }
      else {
        r -= 1
      }
    }
    maxValue
  }
}
