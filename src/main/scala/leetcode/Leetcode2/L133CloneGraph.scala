package leetcode.Leetcode2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object L133CloneGraph {

  def main(args: Array[String]): Unit = {

    var n1 = new Node(1)
    var n2 = new Node(2)
    var n3 = new Node(3)
    var n4 = new Node(4)

    //    n1.neighbors = n1.neighbors.::(n2).::(n4)
    //    n2.neighbors = n2.neighbors.::(n1).::(n3)
    //    n3.neighbors = n3.neighbors.::(n2).::(n4)
    //    n4.neighbors = n4.neighbors.::(n1).::(n3)

    n1.neighbors :+= n2
    n1.neighbors :+= n4
    n2.neighbors :+= n1
    n2.neighbors :+= n3
    n3.neighbors :+= n2
    n3.neighbors :+= n4
    n4.neighbors :+= n1
    n4.neighbors :+= n3

    //    cloneGraph(n1)
    v3(n1)
  }

  def v3(graph: Node): Node = {
    var col1 = Array[Node]()
    col1 :+= graph
    var visited = mutable.Set[Node]()
    var visited1 = Map[Node, Node]()
    var visited2 = ArrayBuffer[Node]()
    val queue = mutable.Queue[Node]()
    queue.enqueue(graph)
    visited1 += (graph -> new Node(graph.value))
    while (queue.nonEmpty) {
      var n = queue.dequeue()
      for (elem <- n.neighbors) {
        if (!visited1.contains(elem)) {
          visited1 += (elem -> new Node(elem.value))
          queue.enqueue(elem)
        }
        visited1(n).neighbors :+= visited1(elem)
      }
    }


    return visited1(graph)


    //1\队列实现
    //    while (queue.nonEmpty) {
    //      var n = queue.dequeue()
    //      visited2 += n
    //      for (elem <- n.neighbors) {
    //        if (!visited2.contains(elem)) {
    //          queue.enqueue(elem)
    //        }
    //      }
    //    }
    //2\数组实现
    //    while (col1.nonEmpty) {
    //      var col2 = Array[Node]()
    //      for (elem <- col1) {
    //        visited2 += elem
    //        for (elem1 <- elem.neighbors) {
    //          if (!visited2.contains(elem1)) {
    //            col2 :+= elem1
    //          }
    //        }
    //      }
    //      col1 = col2
    //    }

    //    visited2.foreach(ele => println(ele.value))
  }

  def cloneGraph(graph: Node) = {
    var visited = Map[Node, Node]()

    def helper(node: Node): Node = {
      if (node == null) {
        return node
      }
      if (visited.contains(node)) {
        return visited(node)
      }
      println(node.value)
      val cloneNode = new Node(node.value)
      visited += (node -> cloneNode)
      for (elem <- node.neighbors) {
        cloneNode.neighbors = cloneNode.neighbors :+ helper(elem)
      }
      return cloneNode
    }

    helper(graph)
  }


  def v2(graph: Node): Node = {
    var map = Map[Node, Node]()

    def helper(node: Node): Node = {
      if (node == null) {
        return null
      }
      if (map.contains(node)) {
        return map(node)
      }
      val cloneNode = new Node(node.value)
      map += (node -> cloneNode)
      for (elem <- node.neighbors) {
        cloneNode.neighbors :+= helper(elem)
      }
      return cloneNode
    }

    helper(graph)

  }

  class Node(var _value: Int) {
    var value: Int = _value
    var neighbors: List[Node] = List()
  }

}
