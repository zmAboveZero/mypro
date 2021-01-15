package leetcode.Leetcode2

import scala.collection.mutable

object L100SameTree {

  def main(args: Array[String]): Unit = {
    var preorder = Array[Int](1, 2)
    //    var preorder = Array[Int](5, 4, 11, 7, 2, 8, 13, 4, 1)
    var inorder = Array[Int](2, 1)
    //    var inorder = Array[Int](7, 11, 2, 4, 5, 13, 8, 4, 1)
    val root1 = buildTree(preorder, inorder)


    var preorder2 = Array[Int](1, 2)
    //    var preorder = Array[Int](5, 4, 11, 7, 2, 8, 13, 4, 1)
    var inorder2 = Array[Int](1, 2)
    //    var inorder = Array[Int](7, 11, 2, 4, 5, 13, 8, 4, 1)
    val root2 = buildTree(preorder2, inorder2)

    println(v1(root1, root2))
  }

  def v1(root1: TreeNode, root2: TreeNode): Boolean = {

    if (root1 == null || root2 == null) {
      return false

    }


    var stack1 = mutable.Stack[TreeNode]()
    var stack2 = mutable.Stack[TreeNode]()
    var temp1 = root1
    var temp2 = root2


    while (temp1 != null || stack1.nonEmpty) {

      while (temp1 != null && temp2 != null) {
        if (temp1.value != temp2.value) {
          return false
        }
        stack1.push(temp1)
        stack2.push(temp2)
        temp1 = temp1.left
        temp2 = temp2.left
      }
      if (temp1 != temp2) {
        return false
      }
      temp1 = stack1.pop()
      temp2 = stack2.pop()


      temp1 = temp1.right
      temp2 = temp2.right
    }

    return true
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


  class TreeNode(_x: Int) {
    var value = _x
    var left: TreeNode = null
    var right: TreeNode = null


  }

}
