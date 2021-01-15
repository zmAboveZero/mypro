package leetcode.Leetcode1

object L21MergeTwoSortedLists {


  def main(args: Array[String]): Unit = {

    //
    val ints1 = Array(1, 2, 3, 3, 4, 5)
    var listNode1 = new ListNode(ints1(0))
    var temp1: ListNode = listNode1
    for (i <- 1 until ints1.length) {
      temp1.next = new ListNode(ints1(i))
      temp1 = temp1.next
    }


    val ints2 = Array(1)
    var listNode2 = new ListNode(ints2(0))
    var temp2: ListNode = listNode2
    for (i <- 1 until ints2.length) {
      temp2.next = new ListNode(ints2(i))
      temp2 = temp2.next
    }


    //    var re = v1(listNode1, listNode2)
    var re2 = v3(listNode1, listNode2)


    var temp = listNode1
    while (temp != null) {
      println(temp.x)
      temp = temp.next
    }


    println("===============")
    //遍历一个链表start================================
    var flag = true
    while (flag) {
      println(listNode1.x)
      if (listNode1.next == null) {
        flag = false
      }
      listNode1 = listNode1.next
    }
    //遍历一个链表end================================


  }

  //================================


  def v3(l1: ListNode, l2: ListNode) = {
    var head1 = l1
    var head2 = l2
    var re = new ListNode()
    var cur: ListNode = re
    while (head1 != null && head2 != null) {
      if (head1.x < head2.x) {
        cur.next = head1
        cur = cur.next
        head1 = head1.next
      } else {
        cur.next = head2
        cur = cur.next
        head2 = head2.next
      }
    }
    if (head2 == null) {
      cur.next = head1
    }
    else {
      cur.next = head2
    }
    re.next
  }

  def v1(l1: ListNode, l2: ListNode): ListNode = {
    if (l1 == null) {
      return l2
    } else if (l2 == null) {
      return l1
    } else if (l1.x < l2.x) {
      l1.next = v1(l1.next, l2)
      return l1
    }
    else {
      l2.next = v1(l1, l2.next)
      return l2
    }
  }

  def v2(l1: ListNode, l2: ListNode): ListNode = {
    if (l1 == null) {
      return l2
    }
    if (l2 == null) {
      return l1
    }
    //    var head1 = l1
    var head2 = l2
    var cur1 = l1
    var cur2 = l2
    var flag1 = true
    while (flag1) {
      var flag2 = true
      //l1操作区--start
      while (flag2) {
        //l2操作区--start
        if (cur1.x <= cur2.x) {
          var temp = cur1.next
          cur1.next = cur2
          head2 = cur1
          cur2 = head2
          cur1 = temp
          flag2 = false
        } else if (cur2.next != null && cur1.x <= cur2.next.x) {
          var temp = cur1.next
          cur1.next = cur2.next
          cur2.next = cur1
          cur1 = temp
          flag2 = false
        } else if (cur2.next == null) {
          cur2.next = cur1
          return head2
        } else {
          cur2 = cur2.next
        }
        //l2操作区--end
      }
      //l1操作区--end
      if (cur1 == null) {
        flag1 = false
      }
    }

    head2


  }


  //    var head1: ListNode = l1
  //    var temp1 = head1
  //    var head2: ListNode = l2
  //    var temp2 = head2
  //    var flag = true
  //    while (temp1.next != null && flag) {
  //      if (temp1.x <= temp2.x) {
  //        head1 = temp1.next
  //        temp1.next = temp2
  //        head2 = temp1
  //        temp1 = head1
  //      } else {
  //        while (temp2.next != null && temp1._x > temp2.next.x) {
  //          temp2 = temp2.next
  //        }
  //        if (temp2.next == null) {
  //          temp2.next = temp1
  //          flag = false
  //        } else {
  //          head1 = temp1.next
  //          temp1.next = temp2.next
  //          temp2.next = temp1
  //          temp1 = head1
  //
  //        }
  //      }
  //
  //
  //    }
  //
  //
  //    while (head2.next != null) {
  //      println(head2.x)
  //      head2 = head2.next
  //    }


  //    while (temp1.next != null) {
  //
  //
  //      var temp = temp1.next
  //
  //
  //      if (temp1.x <= temp2.x) {
  //        temp1.next = temp2
  //      } else {
  //        while (temp1._x >= temp2.next.x) {
  //
  //          var te=temp2.next
  //          te = te.next
  //
  //
  //        }
  //        temp1.next = temp2.next
  //        temp2.next = temp1
  //
  //      }
  //
  //      temp1 = temp
  //
  //    }
  //    while (temp2.next != null) {
  //      temp2 = temp2.next
  //      println("=====")
  //
  //    }


  //    var flag = true
  //    while (flag) {
  //
  //      println("-----------", temp1._x)
  //      temp = temp1.next
  //
  //      while (temp1._x >= temp2._x && temp1._x < temp2.next._x) {
  //        println("==========", temp2.next._x)
  //        temp2 = temp2.next
  //      }
  //      temp1.next = temp2.next
  //      temp2.next = temp1
  //      temp1 = temp
  //      if (temp1.next == null) {
  //        println(temp1._x)
  //        flag = false
  //      }
  //
  //    }

  class ListNode(var _x: Int = 0) {
    var next: ListNode = null
    var x: Int = _x
  }

}



