import java.util.concurrent.{Callable, FutureTask}

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Test6 {
  def main(args: Array[String]): Unit = {
    //
    val result: Callable[String] = new Callable[String] {
      override def call() = {
        ""
      }
    }
    val runnable: Runnable = new Runnable {
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
