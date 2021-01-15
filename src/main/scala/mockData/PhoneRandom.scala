package mockData

object PhoneRandom {
  private val telFirst: Array[String] = "134,135,136,137,138,139,150,151,152,157,158,159,130,131,132,155,156,133,153".split(",")

  def getNum(start: Int, end: Int): Int = (Math.random * (end - start + 1) + start).toInt

  def getTel: String = {
    val index: Int = getNum(0, telFirst.length - 1)
    val first: String = telFirst(index)
    val second: String = String.valueOf(getNum(1, 888) + 10000).substring(1)
    val third: String = String.valueOf(getNum(1, 9100) + 10000).substring(1)
    first + second + third
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 100) {
      val tel: String = getTel
      System.out.println(tel)
    }

  }
}
