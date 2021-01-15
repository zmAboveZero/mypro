package leetcode.Leetcode2

import scala.collection.mutable.ArrayBuffer

object L275BinaryTreePaths {

  def main(args: Array[String]): Unit = {

    var preorder = Array[Int](1, 2, 5, 3)
    var inorder = Array[Int](2, 5, 1, 3)
    var root = builTree(preorder, inorder)
    v1(root)
  }

  def v1(root: TreeNode) = {
    var paths = ArrayBuffer[Array[Int]]()
    var path = Array[Int]()
    helper(root, path, paths)
    for (elem <- paths) {
      println(elem.seq)
    }
    paths.toList.map(ele => ele.mkString("->"))
  }

  def helper(root: TreeNode, path: Array[Int], paths: ArrayBuffer[Array[Int]]): Unit = {
    if (root == null) {
      return
    }
    if (root.left == null && root.right == null) {
      paths += path :+ root.value
      return
    }
    helper(root.left, path :+ root.value, paths)
    helper(root.right, path :+ root.value, paths)
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
