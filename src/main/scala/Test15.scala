import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Edge

object Test15 {
  def main(args: Array[String]): Unit = {
    //    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
    //    val sc = new SparkContext(conf)
    //
    //    val edges = sc.parallelize(List((1, 2), (2, 3), (3, 4), (4, 5), (2, 5), (3, 6), (3, 6), (7, 8), (9, 10)))
    //    println(edges.count())
    //    for (elem <- sc.getExecutorMemoryStatus) {
    //      println(elem)
    //    }
    //    println(new Person("", 2, "").productArity)
    //    println(new Person("", 2, "").productElement(1))
    val jack = new Person("jack", 2, "male", List("a", "b"))

    jack.productIterator.foreach(ele => println(ele))
    println(jack.productArity)

  }

  case class Person(name: String, age: Int, gender: String, jobs: List[String])

}


