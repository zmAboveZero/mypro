package Leetcode1

object L334ReverseString {
  def main(args: Array[String]): Unit = {
    var input = "helloworldP"
    val array: Array[Char] = input.toCharArray


    var start = 0
    var end = array.length - 1

    while (start < end) {
      var temp = array(start)
      array(start) = array(end)
      array(end) = temp
      start += 1
      end -= 1
    }
    println(array.toBuffer)

  }

}
