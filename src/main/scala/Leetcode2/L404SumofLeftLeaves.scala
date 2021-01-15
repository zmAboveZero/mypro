package Leetcode2

import scala.collection.mutable

object L404SumofLeftLeaves {

  def main(args: Array[String]): Unit = {
    var preorder = Array[Int](3, 9, 20, 15, 7)
    var inorder = Array[Int](9, 3, 15, 20, 7)
    var root = builTree(preorder, inorder)
    v1(root)
  }

  def v1(root: TreeNode) = {
    var sum = 0
    var stack = mutable.Stack[TreeNode]()
    var temp = root
    while (temp != null || stack.nonEmpty) {
      if (temp != null) {
        stack.push(temp)
        temp = temp.left
      } else {
        temp = stack.pop()
        if (temp.right == null) {
          sum += temp.value
        }
        temp = temp.right
      }
    }
    println(sum)


  }

  def builTree(preorder: Array[Int], inorder: Array[Int]): TreeNode = {
    if (inorder.isEmpty) {
      return null
    }
    var root = new TreeNode(preorder(0))
    var mid = 0
    while (inorder(mid) != preorder(0)) {
      mid += 1
    }
    root.left = builTree(preorder.slice(1, mid + 1), inorder.slice(0, mid))
    root.right = builTree(preorder.slice(mid + 1, preorder.length), inorder.slice(mid + 1, inorder.length))
    return root
  }

  class TreeNode(x: Int) {
    var value = x
    var left: TreeNode = null
    var right: TreeNode = null
  }

}
