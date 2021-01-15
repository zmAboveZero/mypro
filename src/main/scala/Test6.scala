package com.zm.test

import scala.collection.mutable

object Test6 {
  var preindex = 0
  var preorder: Array[Int] = null
  var inorder: Array[Int] = null
  var indexMap = Map[Int, Int]()

  def main(args: Array[String]): Unit = {
    //    var preorder = Array(3, 8, 12, 2, 10, 15, 7, 14, 9, 6)
    //    var inorder = Array(8, 2, 12, 3, 10, 14, 7, 9, 15, 6)
    var preorder = Array(1, 2, 3, 2, 3)
    var inorder = Array(2, 3, 1, 2, 3)
    var root = createTree(preorder, inorder)
    //    println(isSymmetry(root))
    traversal(root)
  }

  def traversal(root: TreeNode): Unit = {
    if (root == null) {
      return
    }
    traversal(root.left)
    println(root.value)
    traversal(root.right)
  }

  def isSymmetry(root: TreeNode): Boolean = {
    var stack1 = mutable.Stack[TreeNode]()
    var stack2 = mutable.Stack[TreeNode]()
    var temp1 = root
    var temp2 = root
    while ((stack1.nonEmpty && stack2.nonEmpty) || (temp1 != null && temp2 != null)) {
      while (temp1 != null && temp2 != null) {
        stack1.push(temp1)
        stack2.push(temp2)
        temp1 = temp1.left
        temp2 = temp2.right
      }
      temp1 = stack1.pop()
      temp2 = stack2.pop()
      if (temp1.value != temp2.value) {
        return false
      }
      temp1 = temp1.right
      temp2 = temp2.left
    }
    return true
  }


  def createTree(preorder: Array[Int], inorder: Array[Int]) = {
    this.preorder = preorder
    this.inorder = inorder
    for (i <- inorder.indices) {
      indexMap += (inorder(i) -> i)
    }
    var root = helper(0, inorder.length)
    root
  }

  def helper(in_left: Int, in_right: Int): TreeNode = {
    if (in_left == in_right || preindex >= preorder.length) {
      return null
    }
    //    println(preindex)
    var root_val = preorder(preindex)
    var root = new TreeNode(root_val)
    var index = indexMap(root_val)
    preindex += 1
    root.left = helper(in_left, index)
    root.right = helper(index + 1, in_right)
    return root
  }

  class TreeNode(var _value: Int) {
    var value = _value
    var left: TreeNode = null
    var right: TreeNode = null
  }

}
