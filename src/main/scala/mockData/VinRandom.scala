package mockData

import java.util.Random

object VinRandom {

  /** 车架号地区代码数组 */
  val areaArray: Array[String] = Array[String]("1", "2", "3", "6", "9", "J", "K", "L", "R", "S", "T", "V", "W", "Y", "Z", "G")

  /** 车架号中可能出现的字符数组 */
  val charArray: Array[String] = Array[String]("1", "2", "3", "4", "5", "6", "7", "8", "A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L", "M", "N", "P", "R", "S", "T", "V", "W", "X", "Y")

  /** 车架号校验位计算数组 */
  val KVMACTHUP: Array[Array[Char]] = Array[Array[Char]](Array('A', 1), Array('B', 2), Array('C', 3), Array('D', 4), Array('E', 5), Array('F', 6), Array('G', 7), Array('H', 8), Array('I', 0), Array('J', 1), Array('K', 2), Array('L', 3), Array('M', 4), Array('N', 5), Array('O', 0), Array('P', 7), Array('Q', 8), Array('R', 9), Array('S', 2), Array('T', 3), Array('U', 4), Array('V', 5), Array('W', 6), Array('X', 7), Array('Y', 8), Array('Z', 9))
  /** 车架号数据加权数组 */
  val WEIGHTVALUE: Array[Int] = Array[Int](8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2)

  /**
    * 计算车架号的校验位
    *
    * @return
    */
  def getIsuredCode(vin: String): String = {
    val Vin: Array[Char] = vin.toCharArray
    var sum = 0
    var tempValue = 0
    var temp = 0
    for (i <- 0 until (17)) {
      if (Vin(i) >= 'a' && Vin(i) <= 'z') temp = (Vin(i) - 32).toChar
      else if ((Vin(i) >= 'A') && (Vin(i) <= 'Z')) temp = Vin(i)
      else if ((Vin(i) >= '0') && (Vin(i) <= '9')) {
        tempValue = String.valueOf(Vin(i)).toInt
        temp = Vin(i)
      }
      else return "ERROR"
      if ((temp >= 'A') && (temp <= 'Z')) {
        for (j <- 0 until (26)) {
          if (temp == KVMACTHUP(j)(0)) tempValue = KVMACTHUP(j)(1).asInstanceOf[Int]
        }
      }
      sum += tempValue * WEIGHTVALUE(i)
    }
    val reslt: Int = sum % 11
    if (reslt != 10) return String.valueOf(reslt)
    else return "X"
  }

  /**
    * 判断vin是否正确
    *
    * @param vin
    * @return
    */
  def isVin(vin: String): Boolean = {
    val isuredCode: String = getIsuredCode(vin)
    if (vin.substring(8, 9) == isuredCode) true
    else false
  }

  /**
    * 拼接车架号
    *
    * @param beforeStr
    * @param afterStr
    * @return
    */
  def spellVin(beforeStr: String, afterStr: String): String = {
    val vinBuffer = new StringBuffer
    val preVin: String = vinBuffer.append(beforeStr).append("X").append(afterStr).toString
    val isuredCode: String = getIsuredCode(preVin)
    val vin: String = new StringBuffer(beforeStr).append(isuredCode).append(afterStr).toString
    if (isVin(vin)) vin
    else spellVin(beforeStr, afterStr)
  }

  /**
    * 生成随机前缀
    *
    * @return
    */
  def prepareBeforeStr: String = {
    val stringBuffer = new StringBuffer
    stringBuffer.append("VNN")
    for (i <- 0 until 5) {
      stringBuffer.append(getRandomChar(areaArray))
    }
    stringBuffer.toString
  }

  /**
    * 生成随机后缀
    *
    * @return
    */
  def prepareAfterStr: String = {
    val stringBuffer = new StringBuffer
    for (i <- 0 until 3) {
      stringBuffer.append(getRandomChar(charArray))
    }
    stringBuffer.append(prepareNo)
    stringBuffer.toString
  }

  /**
    * 生成随机的生产序号
    *
    * @return
    */
  def prepareNo: String = {
    val random = new Random()
    val numStrBuff = new StringBuffer
    for (i <- 0 until (5)) {
      numStrBuff.append(Integer.toHexString(random.nextInt(16)).toUpperCase)
    }
    numStrBuff.toString
  }

  /**
    * 返回随机字符
    *
    * @return
    */
  def getRandomChar(array: Array[String]): String = charArray((Math.random * 100 % array.length).toInt)

  /**
    * 获取随机的车架号
    *
    * @return
    */
  def getRandomVin: String = {
    val beforeStr: String = prepareBeforeStr
    val afterStr: String = prepareAfterStr
    val vin: String = spellVin(beforeStr, afterStr)
    vin
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 50) {
      System.out.println(VinRandom.getRandomVin)
    }
  }
}
