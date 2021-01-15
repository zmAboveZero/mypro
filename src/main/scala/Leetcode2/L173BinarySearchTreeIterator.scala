package Leetcode2

import scala.collection.mutable.Stack


object L173BinarySearchTreeIterator {

  def main(args: Array[String]): Unit = {
    val data = Array[Int](1, 2, 3, 4, 5, 6, 7, 8)
    val root = sortedArrayToBST(data)

    new BSTIterator(root)

  }

  class BSTIterator(_root: TreeNode) {
    var stack = Stack[TreeNode]()

    push(_root)

    def push(root: TreeNode) = {
      var temp = root
      while (temp != null) {
        stack.push(temp)
        temp = temp.left
      }
    }

    def next(): Int = {
      var temp = stack.pop()
      if (temp.right != null) {
        push(temp.right)
      }
      return temp.value
    }

    def hasNext(): Boolean = {
      stack.length != 0
    }


  }

  def sortedArrayToBST(data: Array[Int]) = {


    def helper(data: Array[Int], left: Int, right: Int): TreeNode = {
      if (left > right) {
        return null
      }
      var mid = (left + right) / 2
      var root = new TreeNode(data(mid))
      root.left = helper(data, left, mid - 1)
      root.right = helper(data, mid + 1, right)
      return root
    }

    helper(data, 0, data.length - 1)
  }

  class TreeNode(_x: Int) {
    var value = _x
    var left: TreeNode = null
    var right: TreeNode = null
  }

}
