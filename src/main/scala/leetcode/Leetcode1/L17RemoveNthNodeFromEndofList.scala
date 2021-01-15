package leetcode.Leetcode1

object L17RemoveNthNodeFromEndofList {

  def main(args: Array[String]): Unit = {
    val ints = Array(1, 2)
    var head = new ListNode(1)
    var cur = head
    for (i <- ints.slice(1, ints.length)) {
      cur.next = new ListNode(i)
      cur = cur.next
    }
    var heo = v2(head, 1)
    var temp = heo
    var flag = true
    while (flag) {
      println(temp.x)
      if (temp.next == null) {
        flag = false
      }
      temp = temp.next
    }
  }

  def v2(head: ListNode, n: Int): ListNode = {

    if (head.next == null && n == 1) {
      return null
    }

    var cur1 = head
    var cur2 = head
    for (i <- 0 until (n)) {
      if (cur1.next != null) {
        cur1 = cur1.next
      }
      else {
        return head.next
      }
    }
    while (cur1.next != null) {
      cur1 = cur1.next
      cur2 = cur2.next
    }
    cur2.next = cur2.next.next
    head


  }

  def v1(head: ListNode, n: Int): ListNode = {
    var map: Map[Int, ListNode] = Map[Int, ListNode]()
    var flag = true
    var cur = head
    var count = 0
    while (flag) {
      map += (count -> cur)
      if (cur.next == null) {
        flag = false
      }
      cur = cur.next
      count += 1
    }


    if (map.size <= n) {
      return head.next
    }
    map(map.size - n - 1).next = map(map.size - n).next
    head
  }

  class ListNode(var _x: Int = 0) {
    var next: ListNode = null
    var x: Int = _x
  }


}
