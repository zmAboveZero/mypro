package test2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Test4 {
  var preindex = 0
  var preorder: Array[Int] = null
  var inorder: Array[Int] = null
  var indexMap = Map[Int, Int]()

  def main(args: Array[String]): Unit = {
    var preorder = Array(3, 8, 12, 2, 10, 15, 7, 14, 9, 6)
    //    var preorder = Array(3, 9, 20, 15, 7)
    var inorder = Array(8, 2, 12, 3, 10, 14, 7, 9, 15, 6)
    //    var inorder = Array(9, 3, 15, 20, 7)
    //    var arr1 = Array[Int](8)
    //    arr1 :+= (99)
    //    var ab = ArrayBuffer[Int](99)
    //    val ints: ArrayBuffer[Int] = ab += (33)
    //    println(ab == ints)
    v1(preorder, inorder)
  }


  def v1(preorder: Array[Int], inorder: Array[Int]): Unit = {
    this.preorder = preorder
    this.inorder = inorder
    for (i <- inorder.indices) {
      indexMap += (inorder(i) -> i)
    }
    var root = helper(0, inorder.length)
    v10(root)
  }

  def v10(root: TreeNode): Unit = {
    var stack = mutable.Stack[TreeNode]()
    var maxValue = 0
    var counter = 1
    stack.push(root)
    while (stack.nonEmpty) {
      var h = stack.pop()
      counter -= 1
      //      println(h.value)
      if (h.right != null) {
        stack.push(h.right)
        counter += 1
      }
      if (h.left != null) {
        stack.push(h.left)
        counter += 1
      }
      maxValue = math.max(maxValue, counter)
    }
    println(maxValue)

  }

  def v9(root: TreeNode): Unit = {
    var temp = root
    var stack = mutable.Stack[TreeNode]()
    var max = 0
    var count = 0
    while (temp != null || stack.nonEmpty) {
      while (temp != null) {
        stack.push(temp)
        count += 1
        println(count)
        temp = temp.left
      }
      temp = stack.pop()
      count -= 1
      temp = temp.right
      if (temp == null) {
        max = math.max(count, max)
      }
    }
    //    println(max)
  }

  def v8(root: TreeNode): Unit = {
    if (root == null) {
      return 0
    }
    var arr1 = ArrayBuffer[TreeNode]()
    arr1 += root
    var count = 0
    while (arr1.nonEmpty) {
      count += 1
      var temp = ArrayBuffer[TreeNode]()
      for (i <- arr1) {
        if (i.left != null) {
          temp += i.left
        }
        if (i.right != null) {
          temp += i.right
        }
      }
      arr1 = temp
    }
    count
  }

  def v6(root: TreeNode): Unit = {
    var stack = mutable.Stack[TreeNode]()
    var temp = root
    stack.push(temp)
    while (stack.nonEmpty || temp != null) {
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


  def v7(root: TreeNode): Unit = {
    var stack = mutable.Stack[TreeNode]()
    stack.push(root)
    while (stack.nonEmpty) {
      var h = stack.pop()
      println(h.value)
      if (h.right != null) {
        stack.push(h.right)
      }
      if (h.left != null) {
        stack.push(h.left)
      }
    }
  }


  def v4(root: TreeNode): Unit = {
    var temp = root
    var coll = Array[TreeNode]()
    var re = Array[Int]()
    coll :+= (temp)
    while (coll.nonEmpty) {
      var coll2 = Array[TreeNode]()
      for (i <- coll) {
        if (i.left != null) {
          coll2 :+= (i.left)
        }
        if (i.right != null) {
          coll2 :+= (i.right)
        }
      }
      println("=====")
      coll.foreach(e => println(e.value))
      coll = coll2
    }
  }


  def v5(root: TreeNode): Int = {
    if (root == null) {
      return 0
    }
    var l = v5(root.left) + 1
    var r = v5(root.right) + 1
    return math.max(l, r)
  }

  def v3(root: TreeNode): Unit = {
    var stack = mutable.Stack[TreeNode]()
    var temp = root
    while (!stack.isEmpty || temp != null) {
      if (temp != null) {
        stack.push(temp)
        //        println(temp.value)
        temp = temp.left
      } else {
        temp = stack.pop()
        temp = temp.right
      }
    }
  }

  def v2(root: TreeNode): Unit = {
    if (root == null) {
      return null
    }
    v2(root.left)
    //    println(root.value)
    v2(root.right)
  }

  def helper(in_left: Int, in_right: Int): TreeNode = {
    if (in_left == in_right) {
      return null
    }
    var root_val = preorder(preindex)
    var root = new TreeNode(root_val)
    preindex += 1
    root.left = helper(in_left, indexMap(root_val))
    root.right = helper(indexMap(root_val) + 1, in_right)
    return root
  }

  class TreeNode(var _value: Int) {
    var value = _value
    var left: TreeNode = null
    var right: TreeNode = null
  }

}
