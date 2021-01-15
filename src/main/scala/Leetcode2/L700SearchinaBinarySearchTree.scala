package Leetcode2

object L700SearchinaBinarySearchTree {


  def main(args: Array[String]): Unit = {
    //    var preorder = Array[Int](6, 2, 0, 4, 3, 5, 8, 7, 9)
    var preorder = Array[Int](5,3,2,1,4,8,7,6,9)
    //    var inorder = Array[Int](0, 2, 3, 4, 5, 6, 7, 8, 9)
    var inorder = Array[Int](1,2,3,4,5,6,7,8,9)
    var root = buildTree(preorder, inorder)

    v1(root, 80)
  }

  def v1(root: TreeNode, target: Int): TreeNode = {
    var temp = root
    while (temp.value != target) {


      if (temp.value > target) {
        temp = temp.left
      } else {
        temp = temp.right
      }
      if (temp == null) {
        return null
      }
    }
    temp
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
