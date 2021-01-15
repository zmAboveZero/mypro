package test3


object Test7 {
  def main(args: Array[String]): Unit = {
    //    Array(Array(1, 2), Array(3, 4), Array(5, 6), Array(7, 8), Array(9, 10)).map { ele => ele.contains(1) }.foreach(println(_))
    //    var list = List((1, List(1, 2, 3, 4, 5)), (2, List(1, 2, 3, 4, 5)))
    //    println(func3(1))
    //    val file = new File("C:\\Users\\zm\\Desktop\\ast\\4486852-ceee048bd13b27dd.png")
    //
    //
    //    println(file.getParent)
    //    println(file.getName)
    //    println(file.getFreeSpace)
    //    println(file.getParentFile)
    //    println(file.getAbsoluteFile)

    //    println(10000000 / 500000 + 1)
    //    println(NumberUtils.isNumber("fsfsad"))
    //    println(NumberUtils.isNumber(null))
    //    println(NumberUtils.isNumber("1234242343"))
    try {
      println(Integer.valueOf("333"))
      println(Integer.valueOf("aaa"))
    } catch {
      case e: Exception =>
        println("-----")
    } finally {
    }
    println(">>>>>>>>>>>>>>>>>>>")
  }

  def func1(num: Int): Int = {
    func2(num)
  }

  def func2(num: Int): Int = {
    func1(num)
  }

  var counter = 0

  def func3(num: Int): Int = {
    counter += 1
    if (counter == 1000) return 0
    func3(num) + num
  }


}
