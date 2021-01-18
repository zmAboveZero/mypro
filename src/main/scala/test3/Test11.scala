package test3

import org.apache.spark.{SparkConf, SparkContext}

object Test11 {
  def main(args: Array[String]): Unit = {
    //    val ss = SparkSession.builder()
    //      .config("spark.testing.memory", "2147480000")
    //      .appName("InitGraph")
    //      .master("local[2]")
    //      .getOrCreate()
    //    import ss.implicits._
    //    println(ss.sparkContext.parallelize(List("1", "2", "3")).toDF("name").map(row => row.getAs[String]("name")).collect().length)
    //    val sc: SparkContext = ss.sparkContext
    //    sc.parallelize(List("1", "2", "3")).zip(sc.parallelize(List(4, 5, 6, 7))).foreach(println(_))



    val t: SparkConf = new SparkConf().setMaster("local").setAppName("t").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(t)
    sc.stop()
    println(sc.parallelize(List(1, 2, 3)).count())

    //    ss.catalog.listTables().show()


    //    val input: RDD[(String, Int)] = sc.parallelize(Array(("tom", 20), ("jerry", 15), ("jack", 23), ("tony", 18), ("mickel", 27)))
    //    input.toDF("name", "age").union(input.toDF("hello", "nihoa")).printSchema()
    //    new SparkContext()
    //    input.m
    //    Graph()

    //    for (elem <- sys.env) {
    //      println(elem)
    //    }
    //    println("+++++++++++++")
    //    for (elem <- sys.props) {
    //      println(elem)
    //    }
    //    println("+++++++++++++")
    //    Array().toSeq.asJava
    //
    //    println(">>>>>>")
    //    for (elem <- System.getProperties.propertyNames().asScala) {
    //      println(elem.toString, System.getProperty(elem.toString))
    //    }
  }

}
