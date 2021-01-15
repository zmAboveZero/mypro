package leetcode.Leetcode1

object L58LengthofLastWord {

  def main(args: Array[String]): Unit = {
    var s = "hello   world"
    println(v1(s))

    //    println(s.substring(5))
    Tuple2(1, 3)
    yy(6, 0)
  }

  def v1(s: String): Int = {
    var in = s.trim

    return in.substring(in.lastIndexOf(" ") + 1).length
  }

  case class yy(x: Int, y: Int)

}

