package leetcode.Leetcode1

object L108ConvertSortedArraytoBinarySearchTree {

  def main(args: Array[String]): Unit = {
    var nums = Array(-10, -3, 0, 5, 9, 10)
    v1(nums)

  }


  def v2(nums: Array[Int]) = {
    helper(nums, 0, nums.length - 1)
  }

  def helper(nums: Array[Int], left: Int, right: Int): TreeNode = {
    if (left > right) {
      return null
    }
    var mid = (left + right) / 2
    var root = new TreeNode(nums(mid))
    root.left = helper(nums, left, mid - 1)
    root.right = helper(nums, mid + 1, right)
    return root
  }







  def v1(nums: Array[Int]) = {
    var rootIndex = nums.length / 2
    var root = new TreeNode(nums(rootIndex))
    var temp = root
    for (i <- 0 until (rootIndex)) {
      temp.left = new TreeNode(nums(rootIndex - i - 1))
      temp = temp.left
    }
    temp = root
    for (i <- rootIndex + 1 to nums.length - 1) {
      temp.right = new TreeNode(nums(i))
      temp = temp.right
    }

    println(root.right.value)

  }


  class TreeNode(var _value: Int) {
    var value: Int = _value
    var left: TreeNode = null
    var right: TreeNode = null


  }


}
