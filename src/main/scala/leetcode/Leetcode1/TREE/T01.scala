package leetcode.Leetcode1.TREE

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object T01 {

  var values = Array(10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 66)

  def main(args: Array[String]): Unit = {
    var tree = Array(1, null, 2, 3)
    val root = new TreeNode(values(0))
    createTree(root, 1)
    //    var re = ArrayBuffer[Int]()
    //    traverTree(root, re)
    //    println(v1(root))
    vt(root)
  }

  def vt(root: TreeNode) = {
    var stack = mutable.Stack[TreeNode]()
    var temp = root
    while (temp != null || !stack.isEmpty) {
      if (temp != null) {
        stack.push(temp)
        temp = temp.left
      } else {
        temp = stack.pop()
        println(temp.value)
        temp = temp.right
      }
    }
  }

  def v1(root: TreeNode): List[Int] = {
    var re = ArrayBuffer[Int]()
    traverTree(root, re).toList
  }

  def traverTree(root: TreeNode, re: ArrayBuffer[Int]): ArrayBuffer[Int] = {
    if (root == null) {
      return re
    }
    traverTree(root.left, re)
    re += root.value
    traverTree(root.right, re)
    return re

  }

  def createTree(parent: TreeNode, n: Int): TreeNode = {
    if (n >= 4) {
      return parent
    }
    val index: Int = n * 2
    parent.left = new TreeNode(values(index - 1))
    parent.right = new TreeNode(values(index))
    createTree(parent.left, index)
    createTree(parent.right, index + 1)
    return parent
  }


  class TreeNode(var _value: Int) {
    var value: Int = _value
    var left: TreeNode = null
    var right: TreeNode = null
  }

}
