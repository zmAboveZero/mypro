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


    val futreTask: FutureTask[String] = new FutureTask(result)


    println(futreTask.isDone)
    new Thread(futreTask).start()
    Thread.sleep(5000)
    println(futreTask.isDone)





    //    println(futreTask.isDone)
    //    new Thread(futreTask).start()
    //    println(futreTask.isDone)
    //    Thread.sleep(6000)
    //    println(futreTask.isDone)
    //    println(futreTask.get())
  }


}
