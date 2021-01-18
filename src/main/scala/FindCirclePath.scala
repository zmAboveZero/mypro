object FindCirclePath {

  def find_cir_starts_with(graph: List[(Int, List[Int])], targetLen: Int, p: List[Int]): Int = {
    var curPath = p

    var curPathLen = curPath.size
    var lastNode = curPath(curPath.length - 1)
    var cnt = 0
    if (curPathLen == targetLen - 1) {
      for (i <- graph.filter(_._1 == lastNode)(0)._2) {
        // if (i > lastNode && !curPath.exists(_ == i) && graph.filter(_._1 == i)(0)._2.exists(_ == curPath(0))) {
        if (i > curPath(1) && !curPath.exists(_ == i) && graph.filter(_._1 == i)(0)._2.exists(_ == curPath(0))) {
          curPath :+= i
          println(curPath)
          cnt += 1
        }
      }
    } else {
      for (i <- graph.filter(_._1 == lastNode)(0)._2) {
        //        if (i > lastNode && !curPath.exists(_ == i)) {
        if (i > curPath(0) && !curPath.exists(_ == i)) {
          cnt += find_cir_starts_with(graph, targetLen, curPath :+ (i))
        }
      }
    }
    return cnt
  }

  def find_cir_of_length(graph: List[(Int, List[Int])], maxLen: Int, targetLen: Int): Int = {
    var cnt = 0
    for (i <- 0 to maxLen - 1) {
      cnt += find_cir_starts_with(graph, targetLen, List(graph(i)._1)) //List(graph(i)._1)//List(i)
    }
    return cnt
  }

  def find_all_cirs(graph: List[(Int, List[Int])], maxLen: Int): Int = {
    var cnt = 0
    //    for (i <- 3 to maxLen) {
    //      cnt += find_cir_of_length(graph, maxLen, i)
    //    }

    find_cir_of_length(graph, maxLen, maxLen)

    return cnt
  }

  def main(args: Array[String]): Unit = {
    //    var graph = List((1, List(2, 3, 7)), (2, List(1, 3)), (3, List(1, 2, 4, 7)), (4, List(3, 5, 6)), (5, List(4, 6)), (6, List(4, 5, 7)), (7, List(1, 3, 6)))
    //    var graph = List((100, List(200, 300, 700)), (200, List(100, 300)), (300, List(100, 200, 400, 700)), (400, List(300, 500, 600)), (500, List(400, 600)), (600, List(400, 500, 700)), (700, List(100, 300, 600, 800)), (800, List(700)))
    //    var graph = List((100, List(200)), (200, List(100, 300)), (300, List(200, 400, 500)), (400, List(300, 600)), (500, List(300, 600)), (600, List(400, 500)))
    //    val edges = List((100, 200),
    //      (200, 300),
    //      (300, 400),
    //      (300, 500),
    //      (400, 600),
    //      (500, 600))
    //    val intToTuples: Map[Int, List[(Int, Int)]] = edges.union(edges.map(_.swap)).groupBy(_._1)
    //    val graph = intToTuples.map(ele => (ele._1, ele._2.map(_._2)))

    //    var graph = List(
    //      (1, List(2, 5)),
    //      (2, List(1, 3)),
    //      (3, List(2, 4)),
    //      (4, List(3, 5)),
    //      (5, List(1, 4))
    //
    //    )
    var graph = List(
      (1, List(2, 4)),
      (2, List(1, 3)),
      (3, List(2, 5)),
      (4, List(1, 5)),
      (5, List(3, 4))
    )
    println(">>>>>>>>>>>", find_all_cirs(graph.toList, 5))

  }

}
