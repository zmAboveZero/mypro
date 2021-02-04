package test3

import java.util.concurrent.{Callable, FutureTask}

object CallTest {
  def main(args: Array[String]): Unit = {
    //
    val result = new Callable[String] {
      override def call() = {
        ""
      }
    }
    val runnable = new Runnable {
      override def run(): Unit = {
      }
    }
    //runnable


    val future = new FutureTask(result)


    println(future.isDone)
    new Thread(future).start()
    Thread.sleep(5000)
    println(future.isDone)


    //    new Thread(future).start()
    //    Thread.sleep(6000)
    //    println(future.isDone)
    //    Thread.sleep(6000)
    //    println(future.isDone)
    //    println(future.get())
  }


}
