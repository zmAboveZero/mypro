package leetcode.Leetcode1

object L28ImplementstrStr {


  def main(args: Array[String]): Unit = {

    println(v2("123978456", "456"))
  }

  def v2(haystack: String, needle: String): Int = {


    val length: Int = needle.length
    var index = 0
    for (i <- 0 to haystack.length - length) {
      if (haystack.slice(i, i + length) == needle) {
        return i
      }

    }
    return -1
  }

  def v1(haystack: String, needle: String): Int = {
    val length: Int = needle.length
    var index = 0
    while (index + length <= haystack.length) {
      val substring: String = haystack.slice(index, index + length)
      if (substring == needle) {
        return index
      } else {
        index += 1
      }
    }

    return -1


  }

}
