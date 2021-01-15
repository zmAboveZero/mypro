package leetcode.Leetcode2

import scala.collection.mutable

object L617MergeTwoBinaryTrees {


  def main(args: Array[String]): Unit = {
    var preorder = Array[Int](5, 4, 11, 7, 2, 8, 13, 4, 1, 10)
    //    var preorder = Array[Int](3, 30, 10, 15, 45)
    var inorder = Array[Int](7, 11, 2, 4, 5, 13, 8, 4, 1, 10)
    //    var inorder = Array[Int](3, 10, 15, 45, 30)
    var root = builTree(preorder, inorder)
    v2(root, root)
  }

  def v2(rt1: TreeNode, rt2: TreeNode): TreeNode = {
    if (rt1 == null) {
      return rt2
    }
    if (rt2 == null) {
      return rt1
    }
    rt1.value += rt2.value
    rt1.left = v2(rt1.left, rt2.left)
    rt1.right = v2(rt1.right, rt2.right)
    return rt1
  }

  def v1(root1: TreeNode, root2: TreeNode): TreeNode = {
    if (root1 == null) {
      return root2
    }
    if (root2 == null) {
      return root1
    }
    var stack = mutable.Stack[(TreeNode, TreeNode)]()
    stack.push((root1, root2))
    while (stack.nonEmpty) {
      var temp = stack.pop()
      temp._1.value += temp._2.value
      if (temp._1.right != null && temp._2.right != null) {
        stack.push((temp._1.right, temp._2.right))
      } else if (temp._1.right == null) {
        temp._1.right = temp._2.right
      }
      if (temp._1.left != null && temp._2.left != null) {
        stack.push((temp._1.left, temp._2.left))
      } else if (temp._1.left == null) {
        temp._1.left = temp._2.left
      }

    }
    root1
  }

  def builTree(preorder: Array[Int], inorder: Array[Int]): TreeNode = {
    if (inorder.isEmpty) {
      return null
    }
    var root = new TreeNode(preorder(0))
    //
    var mid = 0
    while (inorder(mid) != preorder(0)) {
      mid += 1
    }
    //
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
