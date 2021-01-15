package leetcode.Leetcode2

import shapeless.ops.zipper.Root

import scala.collection.mutable

object L543DiameterofBinaryTree {

  def main(args: Array[String]): Unit = {
    val preorder = Array[Int](3, 9, 20, 15, 7)
    val inorder = Array[Int](9, 3, 15, 20, 7)
    val root = buildTree(preorder, inorder)
    //    println(v1(root.left) + v1(root.right))
    v2(root)

  }


  def v2(root: TreeNode): Unit = {
    var temp = root
    val stack = mutable.Stack[TreeNode]()


    temp = l(temp, stack)
    temp = temp.right

      while (temp == null) {
        stack.pop()
        temp = stack.top.right
      }






    temp = l(temp, stack)
    temp = temp.right

    while (temp == null) {
      stack.pop()
      temp = stack.top.right
    }

    stack.top.left



    while (temp == null) {
      stack.pop()

      temp = l(temp, stack)
      temp = stack.top.right
    }




    temp = l(temp, stack)
    temp = temp.right
    while (temp == null) {
      stack.pop()
      temp = stack.top.right
    }
    temp = l(temp, stack)
    temp = temp.right
    while (temp == null) {
      stack.pop()
      temp = stack.top.right
    }
    temp = l(temp, stack)
    temp = temp.right
    if (temp.right == null) {
      stack.pop()
    }

    while (temp == null) {
      stack.pop()
      temp = stack.top.right
    }

    temp = l(temp, stack)
    temp = temp.right
    while (temp == null) {
      temp = stack.pop()
      temp = stack.top.right
    }

    temp




    //==============================
    //    temp = stack.top
    //    temp = temp.right
    //    if (temp == null) {
    //      stack.pop()
    //      temp = stack.top.right
    //    } else {
    //      stack.push(temp)
    //      l(temp, stack)
    //      temp = stack.top.right
    //    }


    //    if (temp != null) {
    //      r(temp, stack)
    //    }
  }

  def l(root: TreeNode, stack: mutable.Stack[TreeNode]): TreeNode = {
    var temp = root
    while (temp != null) {
      stack.push(temp)
      temp = temp.left
    }
    stack.top
  }

  def r(root: TreeNode, stack: mutable.Stack[TreeNode]): Unit = {
    var temp = root
    while (temp != null) {
      if (temp.left != null) {
        l(temp.left, stack)
      }
      stack.push(temp)
      temp = temp.right
    }
  }


  def v1(root: TreeNode): Int = {
    if (root == null) {
      return 0
    }
    var l = v1(root.left) + 1
    var r = v1(root.right) + 1

    return math.max(l, r)

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
