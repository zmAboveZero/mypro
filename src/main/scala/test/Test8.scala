package test

import util.control.Breaks._

object Test8 {
  def main(args: Array[String]): Unit = {
    breakable {
      for (i <- 1 to 10) {
        if (i == 5) {
          break()
        }
        println(i)
      }
    }
  }
}
