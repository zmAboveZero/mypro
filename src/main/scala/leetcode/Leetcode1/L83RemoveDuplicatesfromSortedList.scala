package leetcode.Leetcode1

object L83RemoveDuplicatesfromSortedList {
  def main(args: Array[String]): Unit = {

    //
    val ints1 = Array(1, 1, 1, 3, 3)
    var listNode1 = new ListNode(ints1(0))
    var temp1: ListNode = listNode1
    for (i <- 1 until ints1.length) {
      temp1.next = new ListNode(ints1(i))
      temp1 = temp1.next
    }

    //    println(findNext(listNode1).next.x)
    //    val ints2 = Array(1)
    //    var listNode2 = new ListNode(ints2(0))
    //    var temp2: ListNode = listNode2
    //    for (i <- 1 until ints2.length) {
    //      temp2.next = new ListNode(ints2(i))
    //      temp2 = temp2.next
    //    }
    //
    var re = v1(listNode1)
    var flag = true
    while (flag) {
      println(re.x)
      if (re.next == null) {
        flag = false
      }
      re = re.next
    }
  }

  def v1(head: ListNode): ListNode = {
    if (head == null) {
      return null
    }
    var temp = head
    var flag = true
    while (flag) {
      temp.next = findNext(temp).next
      if (temp.next == null) {
        flag = false
      }
      temp = temp.next
    }
    return head
  }

  def findNext(listNode: ListNode): ListNode = {
    var ln = listNode
    if (ln.next == null || ln.x != ln.next.x) {
      return ln
    }
    ln = ln.next
    findNext(ln)
  }
  class ListNode(var _x: Int = 0) {
    var next: ListNode = null
    var x: Int = _x
  }

}

