package leetcode.Leetcode2

object L559MaximumDepthofNaryTree {

  def main(args: Array[String]): Unit = {
    var preorder = Array[Int](5, 4, 11, 7, 2, 8, 13, 4, 1, 10)
    //    var preorder = Array[Int](3, 30, 10, 15, 45)
    var inorder = Array[Int](7, 11, 2, 4, 5, 13, 8, 4, 1, 10)
    //    var inorder = Array[Int](3, 10, 15, 45, 30)
    var root = builTree(preorder, inorder)
    v1(root)
  }

  def v1(root: TreeNode) = {

    var maxCount = 0
    helper(root, 0)

    def helper(root: TreeNode, count: Int): Unit = {
      if (root == null) {
        maxCount = math.max(maxCount, count)
      }




    }
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
