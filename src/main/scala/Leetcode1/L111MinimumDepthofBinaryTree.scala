package Leetcode1

object L111MinimumDepthofBinaryTree {

  def main(args: Array[String]): Unit = {
    var preorder = Array[Int](9, 3, 3, 8)
    var inorder = Array[Int](3, 9, 3, 8)
    var root = buildTree(preorder, inorder)
    println(v1(root))
  }

  def v1(root: TreeNode): Int = {
    if (root == null) {
      return 0
    }
    var temp1 = Array[TreeNode]()
    temp1 :+= root
    var counter = 0
    while (temp1.nonEmpty) {
      var temp2 = Array[TreeNode]()
      counter += 1
      for (elem <- temp1) {
        if (elem.right == null && elem.left == null) {
          return counter
        }
        if (elem.left != null) {
          temp2 :+= elem.left
        }
        if (elem.right != null) {
          temp2 :+= elem.right
        }
      }
      temp1 = temp2
    }
    return counter
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
