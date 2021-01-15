package leetcode.Leetcode1

import scala.{Char, collection}
import scala.collection.parallel.{immutable, mutable}


object L20ValidParentheses {


  def main(args: Array[String]): Unit = {
    var s = "([)]"
    println(v2(s))

  }


  def v2(s: String): Boolean = {
    var stack1 = collection.mutable.Stack[Char]()
    val col = Map(("(" -> ")"), ("[" -> "]"), ("{" -> "}"), ("}" -> "++"), ("]" -> "++"), (")" -> "++"))
    for (i <- s) {
      if (stack1.isEmpty || i.toString != col(stack1.top.toString)) {
        stack1.push(i)
      } else stack1.pop()
    }
    stack1.isEmpty

  }

  def v1(s: String): Boolean = {
    var stack1 = collection.mutable.Stack[Char]()
    for (i <- s) {
      var temp = i.toString
      if (stack1.isEmpty) {
        stack1.push(i)
      }
      else {
        stack1.top.toString match {
          case "(" => if (temp == ")") stack1.pop() else stack1.push(i)
          case "[" => if (temp == "]") stack1.pop() else stack1.push(i)
          case "{" => if (temp == "}") stack1.pop() else stack1.push(i)
          case _ => return false
        }
      }
    }
    stack1.isEmpty
  }

  def isValid(s: String): Boolean = {
    val stack = new java.util.Stack[Char]


    val map = Map(')' -> '(', ']' -> '[', '}' -> '{')


    s.foreach {
      case l@('(' | '[' | '{') => stack.push(l)
      case r@(')' | ']' | '}') =>
        if (stack.isEmpty) return false
        else if (!(stack.pop() == map(r))) return false
    }


    return stack.isEmpty
  }


}
