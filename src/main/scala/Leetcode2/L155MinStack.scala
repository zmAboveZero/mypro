package Leetcode2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object L155MinStack {


  def main(args: Array[String]): Unit = {
    var stack = new MinStack3()
    stack.push(10)
    stack.push(5)
    stack.push(3)
    stack.push(30)
    stack.push(1)
    stack.push(322)

  }


  class MinStack3() {
    private val stack = mutable.Stack[Int]()

    def push(x: Int) {

    }

    def pop(): Unit = {
      stack.pop()
    }

    def top(): Int = {
      stack.top
    }

    def getMin(): Int = {
      stack.top
    }

  }

  class MinStack2() {
    private val stack = mutable.Stack[Int]()

    def push(x: Int) {
      var arr = ArrayBuffer[Int]()
      if (stack.isEmpty) {
        stack.push(x)
      } else {
        while (stack.nonEmpty && stack.top < x) {
          arr += stack.pop()
        }
        stack.push(x)
        for (elem <- 0 until (arr.length)) {
          stack.push(arr(arr.length - 1 - elem))
        }
      }
    }

    def pop(): Unit = {
      stack.pop()
    }

    def top(): Int = {
      stack.top
    }

    def getMin(): Int = {
      stack.top
    }

  }


  class MinStack() {
    var root: Node = null

    def push(x: Int) {
      var temp = root
      if (temp == null) {
        root = new Node(x)
      } else if (temp.value > x) {
        var tt = new Node(x)
        tt.next = root.next
        root = tt
      } else {


        while (temp.next != null && x > temp.next.value) {
          temp = temp.next
        }
        var t = new Node(x)
        t.next = temp.next
        temp.next = t


      }
    }

    def pop() {
      root = root.next
    }

    def top(): Int = {
      root.value
    }

    def getMin(): Int = {
      root.value
    }

    class Node(x: Int) {
      var value: Int = x
      var next: Node = null
    }

  }

}
