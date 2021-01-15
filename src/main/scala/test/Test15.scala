package test

import org.apache.spark.deploy.{Client, SparkSubmit}

import scala.collection.immutable.HashSet


object Test15 {
  def main(args: Array[String]): Unit = {
    println("===")

    Client
    SparkSubmit




    HashSet() + ("tel") + ("case") match {
      case a if a.equals(HashSet("person", "car")) => println("perosn_car")
      case b if b.equals(HashSet("person", "tel")) => println("person_tel")
      case c if c.equals(HashSet("case", "tel")) => println("case_tel")
      case d if d.equals(HashSet("case", "car")) => println("tel_case")
    }
    println(HashSet() + ("case") + ("tel"))



    var x: Any = Array(1, 2, 3, 4)
    x match {
      case _a if _a == "dd" => println(x + "999999999999")
      case s: String if s == "dd" => println("String")
      //      case s => println(s)
      case 1 | "1" | "one" => println("one ")
      case "two" => println(2)
      case y: Int => println("Int 类型 ")
      case z if z == Array(1, 2, 3, 4) => println("haha")
      //      case Array(1, 2, 3, 4) => println("oo")
      case _ => println("其他")
    }


    Option[Any]() match {
      case Some(55) => println("value")
      case s => println("no")
    }

    //    println(HashSet() + ("a") + ("b"))
    //    println(HashSet() + ("b") + ("a"))

  }
}
