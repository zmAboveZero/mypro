import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

class Test2 {
  def main(args: Array[String]): Unit = {
    val java: util.List[Int] = Array(1, 2, 3, 3, 4).toSeq.asJava
    val scala: mutable.Buffer[Int] = java.asScala
    val array: Array[Int] = scala.toArray

  }

}
