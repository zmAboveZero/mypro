package leetcode.Leetcode2

import scala.collection.mutable.ArrayBuffer

object L235LowestCommonAncestorofaBinarySearchTree {

  def main(args: Array[String]): Unit = {

    //    val nums = Array[Int](1, 2, 3, 4, 5, 6, 7, 8, 9)
    //    val root = buildBST(nums)
    //    traverseTree(root)
    var preorder = Array[Int](6, 2, 0, 4, 3, 5, 8, 7, 9)
    var inorder = Array[Int](0, 2, 3, 4, 5, 6, 7, 8, 9)
    var root = buildTree(preorder, inorder)
    println(v1(root, new TreeNode(2), new TreeNode(4)).value)
  }

  def v1(root: TreeNode, p: TreeNode, q: TreeNode): TreeNode = {
    var a = getNodeInBST(root, p)
    var b = getNodeInBST(root, q)

    for (elem <- a) {
      println(elem.value)
    }
    println("===========")
    for (elem <- b) {
      println(elem.value)
    }
    println("=============")

    var index = 0
    while (index < math.min(a.length, b.length)) {
      if (a(index).value != b(index).value) {
        return a(index - 1)
      }
      index += 1
    }
    return a(index - 1)
  }

  def getNodeInBST(root: TreeNode, p: TreeNode): ArrayBuffer[TreeNode] = {
    var temp = root
    var arr = ArrayBuffer[TreeNode]()
    while (temp.value != p.value) {
      arr += temp
      if (temp.value > p.value) {
        temp = temp.left
      } else {
        temp = temp.right
      }
    }


    return arr
  }

  def traverseTree(root: TreeNode): Unit = {
    if (root == null) {
      return
    }
    traverseTree(root.left)
    println(root.value)
    traverseTree(root.right)
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

  def buildBST(nums: Array[Int]) = {
    helper(nums, 0, nums.length)
  }

  def helper(nums: Array[Int], left: Int, right: Int): TreeNode = {
    if (left >= right) {
      return null
    }
    var mid = (left + right) / 2
    var root = new TreeNode(nums(mid))
    root.left = helper(nums, left, mid)
    root.right = helper(nums, mid + 1, right)
    return root
  }

  class TreeNode(x: Int) {
    var value = x
    var left: TreeNode = null
    var right: TreeNode = null

  }

}
