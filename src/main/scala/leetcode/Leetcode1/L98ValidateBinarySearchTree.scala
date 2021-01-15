package leetcode.Leetcode1

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object L98ValidateBinarySearchTree {
  var values = Array(30, 20, 40, 10, 35, 38, 70)

  def main(args: Array[String]): Unit = {
    //    var tree = Array(1, null, 2, 3)
    val root = new TreeNode(values(0))
    createTree(root, 1)
    println(v1(root))
  }


  def v2(root: TreeNode): Boolean = {


    true
  }


  def v1(root: TreeNode): Boolean = {
    if (root == null) {
      return false
    }
    if (root.right == null && root.left == null) {
      return true
    }
    var inorder = Long.MinValue

    var temp = root
    var stack = mutable.Stack[TreeNode]()
    while (temp != null || !stack.isEmpty) {
      if (temp != null) {
        stack.push(temp)
        temp = temp.left
      } else {
        temp = stack.pop()
        println(inorder)
        if (temp.value <= inorder) {
          return false
        }
        inorder = temp.value
        temp = temp.right
      }
    }
    return true

  }


  def createTree(parent: TreeNode, n: Int): TreeNode = {
    if (n >= 2) {
      return parent
    }
    val index: Int = n * 2
    parent.left = new TreeNode(values(index - 1))
    parent.right = new TreeNode(values(index))
    createTree(parent.left, index)
    createTree(parent.right, index + 1)
    return parent
  }

  class TreeNode(var _value: Any) {
    var value: Int = _value.asInstanceOf[Int]
    var left: TreeNode = null
    var right: TreeNode = null
  }

}
