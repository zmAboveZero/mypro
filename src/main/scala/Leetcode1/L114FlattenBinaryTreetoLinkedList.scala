package Leetcode1

import scala.collection.mutable

object L114FlattenBinaryTreetoLinkedList {


  def main(args: Array[String]): Unit = {
    var preorder = Array[Int](1, 2, 3, 4, 5, 6)
    var inorder = Array[Int](3, 2, 4, 1, 5, 6)
    val root: TreeNode = builderTree(preorder, inorder)
    v1(root)
    traversal2(root)
  }


  def v1(root: TreeNode): Unit = {

    if (root != null) {
      var stack = mutable.Stack[TreeNode]()
      var temp = root
      var root2 = new TreeNode(root.value)
      var temp2 = root2
      while (temp != null || stack.nonEmpty) {
        while (temp != null) {
          stack.push(temp)
          temp2.right = new TreeNode(temp.value)
          temp2 = temp2.right
          temp = temp.left
        }
        temp = stack.pop()
        temp = temp.right
      }
      root.right = root2.right.right
      root.left = null
    }
  }

  def traversal2(root: TreeNode): Unit = {
    var temp = root
    while (temp != null) {
      println(temp.value)
      temp = temp.right
    }
  }

  def traversal(root: TreeNode) = {
    var stack = mutable.Stack[TreeNode]()
    var temp = root
    while (temp != null || stack.nonEmpty) {
      while (temp != null) {
        stack.push(temp)
        println(temp.value)
        temp = temp.left
      }
      temp = stack.pop()
      temp = temp.right
    }
  }


  def builderTree(preorder: Array[Int], inorder: Array[Int]): TreeNode = {
    if (inorder.isEmpty) {
      return null
    }
    //
    var root = new TreeNode(preorder(0))
    //
    var mid = 0
    while (inorder(mid) != preorder(0)) {
      mid += 1
    }
    //
    root.left = builderTree(preorder.slice(1, mid + 1), inorder.slice(0, mid))
    root.right = builderTree(preorder.slice(mid + 1, preorder.length), inorder.slice(mid + 1, inorder.length))
    //
    return root
  }

  class TreeNode(_x: Int) {
    var value = _x
    var left: TreeNode = null
    var right: TreeNode = null
  }

}
