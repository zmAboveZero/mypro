package test

import scala.reflect.ClassTag

object Test9 {

  def main(args: Array[String]): Unit = {

    val myMap: collection.Map[String, Any] = Map("Number" -> 1, "Greeting" -> "Hello World", "Animal" -> new Animal)


    //    println("greetingInt is " + getValueFromMap("Greeting", myMap))
    //    println("greetingInt is " + getValueFromMap1[Integer]("Greeting", myMap))

    fun1[String]("ll")
    //    println(Set[Int]().isInstanceOf[String])
  }


  def getValueFromMap[T: ClassTag](key: String, dataMap: collection.Map[String, Any]): Option[T] =
    dataMap.get(key) match {
      case Some(value: T) => Some(value)
      case _ => None
    }

  def getValueFromMap1[T](key: String, dataMap: collection.Map[String, Any]): Option[T] = {

    //    Array[T]()


    "dd".isInstanceOf[T]


    dataMap.get(key) match {
      case Some(value: T) => Some(value)
      case _ => None
    }
  }


  def fun1[T: ClassTag](tar: T) = {
    val arr: Array[T] = Array[T]()
    val set: Set[T] = Set[T]()
    var list: List[T] = List[T]()
    val map: Map[T, T] = Map[T, T]()
  }


  class Animal {
    override def toString = "I am Animal"
  }

}
