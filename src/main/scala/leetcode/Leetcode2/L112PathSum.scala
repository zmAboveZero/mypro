package leetcode.Leetcode2

import scala.collection.mutable.ArrayBuffer

object L112PathSum {

  def main(args: Array[String]): Unit = {

    var preorder = Array[Int](5, 4, 11, 7, 2, 8, 13, 4, 1)
    var inorder = Array[Int](7, 11, 2, 4, 5, 13, 8, 4, 1)
    val root = buildTree(preorder, inorder)
    v1(root, 22)
  }

  def v1(root: TreeNode, sum: Int) = {
    var arr = ArrayBuffer[Int]()
    helper(root, 0, arr)
    println(arr.contains(sum))
  }


  def helper(root: TreeNode, sum: Int, arr: ArrayBuffer[Int]): Unit = {
    if (root == null) {
      return
    }
    if (root.left == null && root.right == null) {
      arr += (sum + root.value)
      return
    }
    helper(root.left, root.value + sum, arr)
    helper(root.right, root.value + sum, arr)
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
