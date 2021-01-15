package Leetcode1

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object L94BinaryTreeInorderTraversal {
  var values = Array(10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 66)

  def main(args: Array[String]): Unit = {
    var tree = Array(1, null, 2, 3)
    val root = new TreeNode(values(0))
    createTree(root, 1)
    v3(root)

  }


  def v3(root: TreeNode) = {
    var stack = mutable.Stack[TreeNode]()
    var temp = root
    while (temp != null || !stack.isEmpty) {
      if (temp != null) {
        println("qian", temp.value)
        stack.push(temp)
        temp = temp.left
      } else {
        temp = stack.pop()
        temp = temp.right
      }
    }
    println(stack)
  }


  def v2(root: TreeNode) = {

    val arr = new ListBuffer[Int]()
    var stack = mutable.Stack[TreeNode]()
    var temp = root
    //    while (temp.left != null || temp.right != null) {
    //

    //      if (temp.left != null) {
    //        stack.push(temp)
    //      }
    //      //      if (temp.right != null) {
    //      //      }
    //    }
    while (temp != null || !stack.isEmpty) {
      while (temp != null) {
        stack.push(temp)
        temp = temp.left
      }
      temp = stack.pop()
      arr += (temp.value)
      //      println(temp.value)
      temp = temp.right
    }

    arr.toList

  }

  def v1(root: TreeNode) = {
    var re = ArrayBuffer[Int]()
    traverTree(root, re)
    re.toList
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
