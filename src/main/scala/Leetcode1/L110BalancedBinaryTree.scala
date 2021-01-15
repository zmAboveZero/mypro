package Leetcode1

object L110BalancedBinaryTree {
  var preorder: Array[Int] = null
  var inorder: Array[Int] = null
  var preindex = 0
  var indexMap = Map[Int, Int]()


  def main(args: Array[String]): Unit = {
    //    var preorder = Array(3, 9, 20, 15, 7,10)
    var preorder = Array(1, 2)
    //    var inorder = Array(9, 3, 15, 20, 7,10)
    var inorder = Array(2, 1)
    var root = v1(preorder, inorder)
    v3(root)
  }

  def v3(root: TreeNode) = {

    var flag = true

    def v2(root: TreeNode): Int = {
      if (root == null) {
        return 0
      }
      var l = v2(root.left) + 1
      var r = v2(root.right) + 1

      if (math.abs(l - r) > 1) {
        flag = flag && false
      }

      return math.max(l, r)
    }

    v2(root)
    flag
  }


  def v1(preorder: Array[Int], inorder: Array[Int]): TreeNode = {
    for (i <- inorder.indices) {
      indexMap += (inorder(i) -> i)
    }
    this.preorder = preorder
    this.inorder = inorder
    helper(0, inorder.length)
  }

  def helper(in_left: Int, in_right: Int): TreeNode = {
    if (in_left == in_right) {
      return null
    }
    var root_val = preorder(preindex)
    var root = new TreeNode(root_val)
    var index = indexMap(root_val)
    preindex += 1
    root.left = helper(in_left, index)
    root.right = helper(index + 1, in_right)
    return root

  }

  class TreeNode(var _value: Int) {
    var value: Int = _value
    var left: TreeNode = null
    var right: TreeNode = null
  }

}
