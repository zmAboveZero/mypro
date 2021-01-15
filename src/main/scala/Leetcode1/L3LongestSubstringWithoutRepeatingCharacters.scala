package Leetcode1

import scala.collection.mutable.ListBuffer

object L3LongestSubstringWithoutRepeatingCharacters {

  def main(args: Array[String]): Unit = {
    var s = "zzaomeng"

    //    println(s.indexOf("g", 4))

    println(v2(s))
  }


  def v1(s: String): Int = {
    val tmp = ListBuffer[Char]()
    var right = 0
    var map = Map[Char, Int]()
    var maxMap = Map[Char, Int]()
    for (left <- 0 until (s.length)) {
      //      var c = s.charAt(right)
      while (right <= s.length - 1 && !map.keySet.contains(s.charAt(right))) {
        map += (s.charAt(right) -> right)
        right += 1
        maxMap = if (map.size > maxMap.size) map else maxMap
      }


      if (right == s.length) {
        return maxMap.size
      }
      map -= s.charAt(left)
    }
    return 0

  }


  def v2(s: String): Int = {
    var index = 0
    var flag = 0
    var len = 0
    var res = 0
    while (index < s.length) {
      var pos = s.indexOf(s.charAt(index), flag)
      if (pos < index) {
        if (len > res) res = len
        len = index - pos - 1
        flag = pos + 1
      }
      index += 1
      len += 1
    }
    if (len > res) res = len
    return res
  }
}
