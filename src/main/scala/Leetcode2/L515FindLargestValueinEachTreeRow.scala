package Leetcode2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object L515FindLargestValueinEachTreeRow {

  def main(args: Array[String]): Unit = {
    var preorder = Array[Int](5, 4, 11, 7, 2, 8, 13, 4, 1, 10)
    //    var preorder = Array[Int](3, 30, 10, 15, 45)
    var inorder = Array[Int](7, 11, 2, 4, 5, 13, 8, 4, 1, 10)
    //    var inorder = Array[Int](3, 10, 15, 45, 30)
    var root = builTree(preorder, inorder)
    //    v1(root)
    //    v2(root)
    v3(root)
  }

  def v3(root: TreeNode) = {
    var arr1 = Array[TreeNode]()
    arr1 :+= root
    var maxValue = ArrayBuffer[Int]()
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
      maxValue += (arr1.map(ele => ele.value).max)
      arr1 = arr2
    }
    maxValue.toList
  }

  def v1(root: TreeNode): List[Int] = {
    if (root == null) {
      return List()
    }
    var maxArr = ArrayBuffer[Int]()
    var maxCount = 0
    helper(root, 0)
    println(maxArr)

    def helper(root: TreeNode, count: Int): Int = {
      if (root == null) {
        maxCount = math.max(maxCount, count)
        return 0
      }
      maxArr += root.value
      maxArr(count) = math.max(maxArr(count), root.value)
      if (count < maxCount) {
        maxArr = maxArr.take(maxArr.length - 1)
      }
      helper(root.left, count + 1)
      helper(root.right, count + 1)
    }

    return maxArr.toList
  }

  def v2(root: TreeNode): List[Int] = {
    if (root == null) {
      return List()
    }
    var maxMap = Map[Int, Int]()

    def helper(root: TreeNode, count: Int): Int = {
      if (root == null) {
        return 0
      }
      var maxvalue = 0
      if (maxMap.contains(count + 1)) {
        maxvalue = math.max(maxMap(count + 1), root.value)
      } else {
        maxvalue = root.value
      }
      maxMap += (count + 1 -> maxvalue)
      helper(root.left, count + 1)
      helper(root.right, count + 1)
    }

    helper(root, 0)
    maxMap.toArray.sortBy(ele => ele._1).map((_._2)).toList
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
