package Leetcode2

object L199BinaryTreeRightSideView {


  def main(args: Array[String]): Unit = {

    var preorder = Array[Int](5, 4, 11, 7, 2, 8, 13, 4, 1)
    var inorder = Array[Int](7, 11, 2, 4, 5, 13, 8, 4, 1)
    val root = buildTree(preorder, inorder)
    v1(root)
  }

  def v1(root: TreeNode): List[Int] = {
    if (root == null) {
      return List()

    }

    var reList = List[Int]()
    var arr1 = Array[TreeNode]()
    arr1 :+= root
    while (arr1.nonEmpty) {

      reList :+= (arr1.last.value)
      var arr2 = Array[TreeNode]()
      for (elem <- arr1) {
        if (elem.left != null) {
          arr2 :+= elem.left
        }
        if (elem.right != null) {
          arr2 :+= elem.right
        }

      }
      println("====")


      arr1 = arr2
    }

    println(reList)
    reList
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
