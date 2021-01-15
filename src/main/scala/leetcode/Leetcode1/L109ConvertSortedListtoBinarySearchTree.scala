package leetcode.Leetcode1

object L109ConvertSortedListtoBinarySearchTree {

  def main(args: Array[String]): Unit = {
    var head = new ListNode(1)
    v1(head)
  }

  def v1(head: ListNode): TreeNode = {

    if (head == null) {
      return null
    }
    var midNode = findMidNode(head)
    var root: TreeNode = new TreeNode(midNode.x)
    if (head == midNode) {
      return root;
    }
    root.left = v1(head)
    root.right = v1(midNode.next)
    return root
  }

  def findMidNode(head: ListNode): ListNode = {
    var prevNode = head
    var slowNode = head
    var fastNode = head
    while (fastNode != null && fastNode.next != null) {
      prevNode = slowNode
      slowNode = slowNode.next
      fastNode = fastNode.next.next
    }
    if (prevNode != null) {
      prevNode.next = null
    }
    return slowNode
  }


  class ListNode(var _x: Int) {
    var next: ListNode = null
    var x: Int = _x
  }

  class TreeNode(var _value: Int) {
    var left: TreeNode = null
    var right: TreeNode = null
    var value = _value

  }

}
