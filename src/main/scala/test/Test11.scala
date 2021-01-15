package test

object Test11 {

  def main(args: Array[String]): Unit = {
    var arr = Array(1, 3, 4, 5, 6, 7)
    fun(arr)
  }


  def fun(arr: Array[Int]): Unit = {
    println("hell")
    var he = arr
    println("hell")
    for (i <- he) {
      arr
      println(i)
    }
  }
}
