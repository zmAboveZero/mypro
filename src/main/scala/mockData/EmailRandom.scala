package mockData

object EmailRandom {

  private val email_suffix: Array[String] = "@gmail.com,@yahoo.com,@msn.com,@hotmail.com,@aol.com,@ask.com,@live.com,@qq.com,@0355.net,@163.com,@163.net,@263.net,@3721.net,@yeah.net,@googlemail.com,@126.com,@sina.com,@sohu.com,@yahoo.com.cn".split(",")
  var base = "abcdefghijklmnopqrstuvwxyz0123456789"

  def getNum(start: Int, end: Int): Int = (Math.random * (end - start + 1) + start).toInt

  /**
    * 返回Email
    *
    * @param lMin 最小长度
    * @param lMax 最大长度
    * @return
    */
  def getEmail(lMin: Int, lMax: Int): String = {
    val length: Int = getNum(lMin, lMax)
    val sb = new StringBuffer
    for (i <- 0 to length) {
      val number: Int = (Math.random * base.length).toInt
      sb.append(base.charAt(number))
    }
    sb.append(email_suffix((Math.random * email_suffix.length).toInt))
    sb.toString
  }

  //  代码源于网络 由kingYiFan整理  create2019/05/24
  def main(args: Array[String]): Unit = {
    for (i <- 0 to 10) {
      val email: String = getEmail(1, i)
      System.out.println(email)
    }
  }
}
