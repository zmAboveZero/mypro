package leetcode.Leetcode1

object L09PalindromeNumber {

  def main(args: Array[String]): Unit = {
    val x = 12300321
    var start = 0
    val array: Array[Char] = x.toString.toCharArray

    var end = array.length - 1

    while (start < end) {

      if (array(start) != array(end)) {
        //        println("fasle")
        return false

      }
      start += 1
      end -= 1
    }
    //    println("true")
    return true

  }

}
