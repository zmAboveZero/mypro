package Leetcode2

import scala.collection.mutable

object L145BinaryTreePostorderTraversal {


  def main(args: Array[String]): Unit = {
    val preorder = Array[Int](3, 9, 20, 15, 7)
    val inorder = Array[Int](9, 3, 15, 20, 7)
    val root = buildTree(preorder, inorder)
    v1(root)
  }

  def v1(root: TreeNode) = {
    var stack = mutable.Stack[TreeNode]()
    var temp = root
    while (temp != null || stack.nonEmpty) {














      pushleft(stack, temp)

      if (temp.left != null) {
        pushleft(stack, temp)
      } else {
        pushright(stack, temp)
      }
      temp = stack.top.right



















      def pushleft(stack: mutable.Stack[TreeNode], root: TreeNode): Unit = {
        var temp = root
        while (temp != null) {
          stack.push(temp)
          temp = temp.left
        }
      }


      def pushright(stack: mutable.Stack[TreeNode], root: TreeNode): Unit = {
        var temp = root
        while (temp != null) {
          stack.push(temp)
          temp = temp.right
        }

      }





      //      temp=stack.pop()
      //      temp=temp.right

      temp = stack.top
      if (temp.right == null) {
        stack.pop()
        temp = stack.top
      } else {
        stack.push(temp.right)
        temp = temp.right
      }


    }


    //    while (temp != null || stack.nonEmpty) {
    //      if (temp != null) {
    //        stack.push(temp)
    //        temp = temp.left
    //        //        if (temp.right == null) {
    //        //          stack.pop()
    //        //        }
    //
    //      } else {
    //        temp = stack.top
    //        if (temp.right == null) {
    //          stack.pop()
    //          temp = stack.top
    //        }
    //
    //      }
    //    }
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
    root.right = buildTree(preorder.slice(mid + 1, preorder.length), inorder.slice(mid + 1, inorder.length))
    return root
  }

  class TreeNode(x: Int) {
    var value = x
    var left: TreeNode = null
    var right: TreeNode = null

  }

}
