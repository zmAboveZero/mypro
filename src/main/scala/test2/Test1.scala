package test2

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

object Test1 {
  def main(args: Array[String]): Unit = {

    var tes = ArrayBuffer[Int]()
    //    tes += null
    //    println(tes.seq)


    val schema = StructType(List(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType)
    ))
    var setarray = ArrayBuffer[Set[Long]](Set(0))
    for (set <- setarray) {
      println("==")
      if (set.contains(9)) {
      } else {
        setarray += Set(9)
      }
    }
    println(setarray.size)
  }


}
