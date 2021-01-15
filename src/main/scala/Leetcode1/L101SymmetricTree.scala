package Leetcode1

import scala.collection.mutable

object L101SymmetricTree {

  def main(args: Array[String]): Unit = {
    //    var preorder = Array(3, 9, 20, 15, 7)
    var preorder = Array(1, 2, 3, 2, 3)
    //    var inorder = Array(9, 3, 15, 20, 7)
    var inorder = Array(2, 3, 1, 2, 3)
    var root = builderTree(preorder, inorder)
    //    traversalTree(root)
    println(v1(root))
  }

  def v1(root: TreeNode): Boolean = {
    var stack1 = mutable.Stack[TreeNode]()
    var stack2 = mutable.Stack[TreeNode]()
    var temp1 = root
    var temp2 = root
    while ((temp1 != null && temp2 != null) || (stack1.nonEmpty && stack2.nonEmpty)) {
      if ((temp1 == null && temp2 != null) || (temp1 != null && temp2 == null)) {
        return false
      }
      if (temp1 != null && temp2 != null) {
        if (temp1.value != temp2.value) {
          return false
        }
        stack1.push(temp1)
        stack2.push(temp2)
        temp1 = temp1.left
        temp2 = temp2.right
      } else {
        temp1 = stack1.pop()
        temp2 = stack2.pop()
        temp1 = temp1.right
        temp2 = temp2.left
      }

    }
    return true
  }

  def traversalTree(root: TreeNode) = {
    var temp = root
    var stack = mutable.Stack[TreeNode]()
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

  def builderTree(preorder: Array[Int], inorder: Array[Int]): TreeNode = {
    if (inorder.isEmpty) {
      return null
    }
    //赋值value
    var root = new TreeNode(preorder(0))
    //查找inorder中root.value的索引
    var mid = 0
    while (inorder(mid) != preorder(0)) {
      mid += 1
    }
    //根据索引切分出左右两个部分,然后呢递归
    root.left = builderTree(preorder.slice(1, mid + 1), inorder.slice(0, mid))
    root.right = builderTree(preorder.slice(mid + 1, preorder.length), inorder.slice(mid + 1, inorder.length))
    return root
  }

  class TreeNode(_x: Int) {
    var value = _x
    var left: TreeNode = null
    var right: TreeNode = null
  }

}
