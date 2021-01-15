import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.PrimitiveVector

object Test12 {
  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
//    val sc = new SparkContext(conf)
//    val graph: Graph[Double, Int] = GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((id, _) => id.toDouble)
//    // Compute the number of older followers and their total age
//    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
//      triplet => { // Map Function
//        if (triplet.srcAttr > triplet.dstAttr) {
//          // Send message to destination vertex containing counter and age
//          triplet.sendToDst((1, triplet.srcAttr))
//        }
//      },
//      // Add counter and age
//      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
//    )
//    // Divide total age by number of older followers to get average age of older followers
//    val avgAgeOfOlderFollowers: VertexRDD[Double] =
//      olderFollowers.mapValues((id, value) =>
//        value match {
//          case (count, totalAge) => totalAge / count
//        })
//    // Display the results
//    avgAgeOfOlderFollowers.collect.foreach(println(_))


    //    val vertexArray = Array(
    //      (1L, ("Alice", 28)),
    //      (2L, ("Bob", 27)),
    //      (3L, ("Charlie", 65)),
    //      (4L, ("David", 42)),
    //      (5L, ("Ed", 55)),
    //      (6L, ("Fran", 50))
    //    )
    //    val edgeArray = Array(
    //      Edge(2L, 1L, 7),
    //      Edge(2L, 4L, 2),
    //      Edge(3L, 2L, 4),
    //      Edge(3L, 6L, 3),
    //      Edge(4L, 1L, 1),
    //      Edge(5L, 2L, 2),
    //      Edge(5L, 3L, 8),
    //      Edge(5L, 6L, 3)
    //    )
    //
    //    //构造vertexRDD和edgeRDD
    //    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    //    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    //
    //    //构造图Graph[VD,ED]
    //    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    //
    //
    //    graph.aggregateMessages[Int](_.sendToSrc(1), _ + _).foreach(println)
    //
    //
    //    println("找出图中年龄大于30的顶点：")
    //    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
    //      case (id, (name, age)) => println(s"$name is $age")
    //    }
    //    //边操作：找出图中属性大于5的边
    //    println("找出图中属性大于5的边：")
    //    graph.edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    //
    //    //triplets操作，((srcId, srcAttr), (dstId, dstAttr), attr)
    //    println("列出边属性>5的tripltes：")
    //    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
    //      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    //    }
    //    //Degrees操作
    //    println("找出图中最大的出度、入度、度数：")
    //
    //    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    //      if (a._2 > b._2) a else b
    //    }
    //
    //    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))
    //
    //
    //    println("顶点的转换操作，顶点age + 10：")
    //    graph.mapVertices { case (id, (name, age)) => (id, (name, age + 10)) }.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    //    println("边的转换操作，边的属性*2：")
    //    graph.mapEdges(e => e.attr * 2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    //
    //    //
    //    println("顶点年纪>30的子图：")
    //    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
    //    println("子图所有顶点：")
    //    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    //    println("子图所有边：")
    //    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    //
    //    //
    //    val inDegrees: VertexRDD[Int] = graph.inDegrees
    //    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
    //    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0) }
    //    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
    //      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    //    }.outerJoinVertices(initialUserGraph.outDegrees) {
    //      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    //    }
    //    println("连接图的属性：")
    //    userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))
    //    println("出度和入读相同的人员：")
    //    userGraph.vertices.filter {
    //      case (id, u) => u.inDeg == u.outDeg
    //    }.collect.foreach {
    //      case (id, property) => println(property.name)
    //    }
    //
    //
    //
    //    println("找出年纪最大的追求者：")
    //    val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)](
    //      // 将源顶点的属性发送给目标顶点，map过程
    //      triplet => { // Map Function
    //        if (triplet.srcAttr > triplet.dstAttr) {
    //          // Send message to destination vertex containing counter and age
    //          triplet.sendToDst((1, triplet.srcAttr))
    //        }
    //      },
    //      // 得到最大追求者，reduce过程
    //      (a, b) => if (a._2 > b._2) a else b
    //    )
    //    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
    //      optOldestFollower match {
    //        case None => s"${user.name} does not have any followers."
    //        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
    //      }
    //    }.collect.foreach { case (id, str) => println(str) }
    //
    //
    //    println("找出5到各顶点的最短：")
    //    val sourceId: VertexId = 5L // 定义源点
    //    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    //    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
    //      (id, dist, newDist) => math.min(dist, newDist),
    //      triplet => { // 计算权重
    //        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
    //          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    //        } else {
    //          Iterator.empty
    //        }
    //      },
    //      (a, b) => math.min(a, b) // 最短距离
    //    )
    //    println(sssp.vertices.collect.mkString("\n"))
//    sc.stop()
  }

}

