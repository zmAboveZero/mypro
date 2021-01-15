package leetcode.Leetcode1

import scala.collection.mutable

object L105ConstructBinaryTreefromPreorderandInorderTraversal {

  var preorder: Array[Int] = null
  var inorder: Array[Int] = null
  var preindex = 0
  var indexMap = Map[Int, Int]()

  def main(args: Array[String]): Unit = {

    var preorder = Array(3, 9, 20, 15, 7)
    var inorder = Array(9, 3, 15, 20, 7)

    //    println(inorder.slice(1, 3).seq)

    var root = v4(preorder, inorder)
    //        var root = v1(preorder, inorder)
    v3(root)
    //    inorder :+= (55)
  }

  def v4(preorder: Array[Int], inorder: Array[Int]): TreeNode = {
    if (inorder.isEmpty) {
      return null
    }
    var root = new TreeNode(preorder(0))
    var mid = 0
    while (preorder(0) != inorder(mid)) {
      mid += 1
    }
    root.left = v4(preorder.slice(1, mid + 1), inorder.slice(0, mid))
    root.right = v4(preorder.slice(mid + 1, preorder.length), inorder.slice(mid + 1, inorder.length))
    return root
  }

  def v3(root: TreeNode) = {

    var stack = mutable.Stack[TreeNode]()
    var temp = root
    while (!stack.isEmpty || temp != null) {
      if (temp != null) {
        stack.push(temp)
        temp = temp.left
      } else {
        temp = stack.pop()
        println(temp.value)
        temp = temp.right
      }
    }
  }

  def v2(root: TreeNode): Unit = {

    if (root == null) {
      return
    }
    println(root.value)
    v2(root.left)
    v2(root.right)
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
