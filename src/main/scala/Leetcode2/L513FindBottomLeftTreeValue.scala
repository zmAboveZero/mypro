package Leetcode2

object L513FindBottomLeftTreeValue {

  def main(args: Array[String]): Unit = {
    var preorder = Array[Int](5, 4, 11, 7, 2, 8, 13, 4, 1)
    var inorder = Array[Int](7, 11, 2, 4, 5, 13, 8, 4, 1)
    val root = buildTree(preorder, inorder)
    v1(root)
    v2(root)
  }

  def v2(root: TreeNode): Int = {
    var arr1 = Array[TreeNode](root)
    while (arr1.nonEmpty) {
      var arr2 = Array[TreeNode]()
      for (elem <- arr1) {
        if (elem.left != null) {
          arr2 :+= elem.left
        }
        if (elem.right != null) {
          arr2 :+= elem.right
        }
      }
      if (arr2.isEmpty) {
        return arr1.head.value
      }
      arr1 = arr2
    }
    return 0
  }

  def v1(root: TreeNode): Int = {
    var max = (0, root.value)

    def helper(root: TreeNode, count: Int): Unit = {
      if (root == null) {
        return
      }
      if (root.left == null && root.right == null) {
        if (count > max._1) {
          max = (count, root.value)
        }
        return
      }
      helper(root.left, count + 1)
      helper(root.right, count + 1)
    }

    helper(root, 0)
    max._2
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
