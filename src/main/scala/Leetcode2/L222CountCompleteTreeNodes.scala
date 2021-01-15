package Leetcode2

object L222CountCompleteTreeNodes {

  def main(args: Array[String]): Unit = {

    //    var preorder = Array[Int](1, 2, 4, 5, 3, 6)
    var preorder = Array[Int](1)
    //    var inorder = Array[Int](4, 2, 5, 1, 6, 3)
    var inorder = Array[Int](1)
    val root = buildTree(preorder, inorder)
    println(v1(root))
  }

  def v1(root: TreeNode): Int = {
    var arr1 = Array[TreeNode]()
    arr1 :+= root
    var layers = 0
    while (arr1.nonEmpty) {
      layers += 1
      println("===")
      var length = 0
      var arr2 = Array[TreeNode]()
      for (elem <- arr1) {
        if (elem.left != null) {
          length += 1
          arr2 :+= elem.left
        }
        if (elem.right != null) {
          length += 1
          arr2 :+= elem.right
        }
      }
      if (arr2.length < math.pow(2, layers)) {
        println(layers)
        println(length)
        return (math.pow(2, layers) - 1 + length).toInt
      }
      arr1 = arr2
    }
    return math.pow(2, layers).toInt
  }

  def buildTree(preorder: Array[Int], inorder: Array[Int]): TreeNode = {
    if (inorder.isEmpty) {
      return null
    }
    var root = new TreeNode(preorder(0))
    var mid = 0
    while (preorder(0) != inorder(mid)) {
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
