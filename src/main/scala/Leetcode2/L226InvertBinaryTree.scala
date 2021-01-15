package Leetcode2

import scala.collection.mutable

object L226InvertBinaryTree {

  def main(args: Array[String]): Unit = {

    val preorder = Array[Int](4, 2, 1, 3, 7, 6, 9)
    //    val preorder = Array[Int]()
    val inorder = Array[Int](1, 2, 3, 4, 6, 7, 9)
    //    val inorder = Array[Int]()
    var root = buildTree(preorder, inorder)
    traversal(root)
    println("======")
    traversal(v2(root))
  }

  def v2(root: TreeNode): TreeNode = {
    if (root == null) {
      return null
    }
    var nums1 = Array[TreeNode]()
    nums1 :+= root
    while (nums1.nonEmpty) {
      var temp = Array[TreeNode]()
      for (elem <- nums1) {
        var temp1: TreeNode = null
        temp1 = elem.right
        elem.right = elem.left
        elem.left = temp1
        //=========================
        if (elem.left != null) {
          temp :+= elem.left
        }
        if (elem.right != null) {
          temp :+= elem.right
        }
        //=========================
      }
      nums1 = temp
    }
    root
  }


  def v1(root: TreeNode): TreeNode = {
    if (root == null) {
      return null
    }
    var temp: TreeNode = null
    temp = root.right
    root.right = root.left
    root.left = temp
    v1(root.left)
    v1(root.right)
    return root
  }

  def traversal(root: TreeNode) = {
    var stack = mutable.Stack[TreeNode]()
    var temp = root
    while (temp != null || stack.nonEmpty) {
      while (temp != null) {
        stack.push(temp)
        temp = temp.left
      }
      temp = stack.pop()
      println(temp.value)
      temp = temp.right
    }


  }

  def buildTree(preorder: Array[Int], inorder: Array[Int]): TreeNode = {
    if (inorder.isEmpty) {
      return null
    }
    var root = new TreeNode(preorder(0))
    var mid = 0
    while (inorder(mid) != preorder(0)) {
      mid += 1
    }
    root.left = buildTree(preorder.slice(1, mid + 1), inorder.slice(0, mid))
    root.right = buildTree(preorder.slice(mid + 1, inorder.length), inorder.slice(mid + 1, inorder.length))
    return root
  }

  class TreeNode(_x: Int) {
    var value = _x
    var left: TreeNode = null
    var right: TreeNode = null
  }

}
