package test3

object Test19 {
  var counter = 0

  def main(args: Array[String]): Unit = {
    try {
      fun()
    } catch {
      case e: Exception => print("Exception")
      case e: Error => print("Error")
    }


  }

  def fun(): Unit = {
    val p = 1000000000
    fun()
    print("fsafdsafsa")
  }

  //  def fun(): String = {
  //    val p = 1000000000
  //    fun()
  //    return "hello"
  //  }
}
