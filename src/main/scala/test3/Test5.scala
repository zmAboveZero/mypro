package test3

object Test5 {
  def main(args: Array[String]): Unit = {
    val start: Long = System.currentTimeMillis()
    val runnable: Runnable = new Runnable {
      var name = ""

      override def run(): Unit = {
        println("1")
        println("2")
        Thread.sleep(5000)
        println("3")
        name = "4"
      }
    }
    val workerThread = new Thread(runnable)
    workerThread.start()
    workerThread.join()

    //println(">>>>" + workerThread.name)
    println("hello")
    // Thread.sleep(5000)
  }

}


class OnLineShopping extends Runnable {
  var name = ""

  override def run(): Unit = {

    println("1")
    println("2")
    Thread.sleep(5000)
    println("3")
    name = "4"
  }
}