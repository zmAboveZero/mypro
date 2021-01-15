package test

object Test14 {
  def main(args: Array[String]): Unit = {
    val clazz = Class.forName("test.Test16")
    println(clazz.getConstructors.length)
    println(clazz.getMethods.length)
    clazz.getMethods.toBuffer(0).invoke(null, Array[String]())
  }

}
