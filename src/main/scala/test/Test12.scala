package test

object Test12 {
  var count = 0

  def main(args: Array[String]): Unit = {
    println(fun(0))
  }


  def fun(n: Int): Int = {
    if (n == 10) {
      return 1
    }
//    fun2()
    fun(n + 1)
    return 0
  }

  def fun2(): Unit = {

    var i = 10
  }
}
