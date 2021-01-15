package Leetcode1

import scala.collection.mutable.ArrayBuffer

object L01TwoNumsSums {

  def main(args: Array[String]): Unit = {
    var nums = Array(1, 9, 4, 4, 56, 90)
    //    println(nums.exists(e => e==56))
    //    println(nums.seq)
    //    println(nums.toSeq)
    //    println(nums.count(e => e == 3))
    //    println(nums.array.seq)
    //    println(nums.tail.seq)
    //    println(nums.update(0, 1000))

    //    nums(0)=100
    //    println(nums.takeRight(2).seq)

    //    println(nums.clone())
    //    println(nums.dropWhile(e => e != 4).seq)

    //    println(nums.indexWhere(e => e == 4))

    //    println(nums.deep)
    //        println(nums.elemTag)

    var s = "hello world"
    println(nums.repr.toBuffer)
    var target = 8
    0 until 10
    //    println(nums.canEqual(363))
    println(0 to 10 % 2)
//    var s = "hello world"
    Map()
    Map(1 -> 2).foreach(e => println(e.getClass))
    nums
    1 -> 2

    var m=Map(1->2)

    //    val i = Array[Int]().toBuffer
    //    i += (33)
    //    println(i)

    //    println(v2(nums, target).toBuffer)
    //    println(v3(nums, target).toBuffer)
    //    v3(nums, target)
    //    v2(nums, target)
    //    test1()

    //    println(v4(nums, target).toBuffer)


  }

  def v4(nums: Array[Int], target: Int): Array[Int] = {

    var map = Map[Int, Int]()
    for (i <- 0 until (nums.length)) {
      var re = target - nums(i)
      if (map.contains(re)) {
        return Array(i, map(re)).sorted
      }
      map += (nums(i) -> i)
    }

    null
  }

  def test1() = {

    Array("a")

    val arr = new Array[Int](1000000)
    var list = List[Int]()
    var set = Set[Int]()
    var map = Map[Int, Int]()
    var arrayBuffer = ArrayBuffer[Int]()
    var t0 = System.currentTimeMillis()
    for (i <- 0 until (1000000)) {
      arr(i) = i
    }


    var t1 = System.currentTimeMillis()
    println("arr----------", t1 - t0)

    //    for (i <- 0 until (100000)) {
    //      list :+= (i)
    //    }

    var t2 = System.currentTimeMillis()
    println("list----------", t2 - t1)

    for (i <- 0 until (1000000)) {
      set += (i)
    }
    var t3 = System.currentTimeMillis()
    println("set----------", t3 - t2)

    for (i <- 0 until (1000000)) {
      map += (i -> i)
    }
    var t4 = System.currentTimeMillis()
    println("map----------", t4 - t3)


    for (i <- 0 until (1000000)) {
      arrayBuffer += (i)
    }
    var t18 = System.currentTimeMillis()
    println("arraybuff----------", t18 - t4)


    var t5 = System.currentTimeMillis()
    val bool1: Boolean = arr.contains(999999)
    var t6 = System.currentTimeMillis()
    val bool2: Boolean = list.contains(999999)
    var t7 = System.currentTimeMillis()
    val bool3: Boolean = set.contains(999999)
    var t8 = System.currentTimeMillis()
    val bool4: Boolean = map.contains(999999)
    var t9 = System.currentTimeMillis()
    val bool5: Boolean = arrayBuffer.contains(999999)
    var t10 = System.currentTimeMillis()
    //
    //
    println(bool1, bool2, bool3, bool4)
    println(t6 - t5)
    println(t7 - t6)
    println(t8 - t7)
    println(t9 - t8)
    println(t10 - t9)

    //    println(arr.length)
    //    println(list.length)
    //    println(set.size)
    //    println(map.size)


  }

  def v3(nums: Array[Int], target: Int): Array[Int] = {
    val start = System.currentTimeMillis()
    for (i <- 0 until (nums.length)) {
      var diff = target - nums(i)
      for (j <- i + 1 until (nums.length)) {
        if (nums(j) == diff) {
          Thread.sleep(100)
          println(System.currentTimeMillis() - start)
          return Array(i, j)
        }
      }
    }
    return Array(0)
  }

  def v2(nums: Array[Int], target: Int): Array[Int] = {
    val start = System.currentTimeMillis()
    for (i <- 0 until (nums.length)) {
      var temp = nums(i)
      for (j <- i + 1 until (nums.length)) {
        if (temp + nums(j) == target) {
          Thread.sleep(100)
          println(System.currentTimeMillis() - start)
          return Array(i, j)
        }
      }
    }

    return Array(0)
  }


  def v1(nums: Array[Int], target: Int) = {


    val start = System.currentTimeMillis()


    var coll = new ArrayBuffer[Int]()
    var flag = true
    for (i <- nums) {
      var index = nums.indexOf(i)
      for (j <- nums.drop(index + 1) if flag) {
        if (i + j == target) {
          coll += nums.indexOf(i)
          if (i == j) {
            nums(nums.indexOf(i)) = Int.MaxValue
          }
          coll += nums.indexOf(j)
          flag = false
        }
      }


    }
    coll.toArray


    println(System.currentTimeMillis() - start)

  }


}
