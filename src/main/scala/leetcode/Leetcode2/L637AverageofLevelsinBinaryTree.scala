package leetcode.Leetcode2

import scala.collection.mutable.ArrayBuffer

object L637AverageofLevelsinBinaryTree {

  def main(args: Array[String]): Unit = {
    var preorder = Array[Int](3, 9, 20, 15, 7)
    //    var preorder = Array[Int](5, 4, 11, 7, 2, 8, 13, 4, 1, 10)
    //    var preorder = Array[Int](3, 30, 10, 15, 45)
    var inorder = Array[Int](9, 3, 15, 20, 7)
    //    var inorder = Array[Int](7, 11, 2, 4, 5, 13, 8, 4, 1, 10)
    //    var inorder = Array[Int](3, 10, 15, 45, 30)
    var root = builTree(preorder, inorder)
    v1(root)
  }

  def v1(root: TreeNode): Array[Double] = {
    var maxLayer = 0
    var meansLayer = ArrayBuffer[Double]()
    helper(root, 0)

    def helper(root: TreeNode, layer: Int): Unit = {
      if (root == null) {
        maxLayer = math.max(maxLayer, layer)
        return
      }
      meansLayer += root.value
      println("======")
      println(layer)
      println(meansLayer(layer) * layer + root.value)


      if (layer >= maxLayer) {
        meansLayer(layer) = root.value
      } else {
        //每层求和
        meansLayer(layer) = meansLayer(layer) + root.value
        //每层求均值,不行,因为每层的个数并不是layer
        //        meansLayer(layer) = (meansLayer(layer) * (layer - 1) + root.value) / (layer)
        meansLayer = meansLayer.take(meansLayer.length - 1)
      }

      //      meansLayer(layer) = meansLayer(layer) / (layer + 1)
      //      meansLayer(layer) = math.max(meansLayer(layer), root.value)
      //      if (layer < maxLayer) {
      //        meansLayer = meansLayer.take(meansLayer.length - 1)
      //      }
      helper(root.left, layer + 1)
      helper(root.right, layer + 1)
    }

    println(meansLayer)
    meansLayer.toArray
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
