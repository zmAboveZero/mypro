package test

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "2147480000").setMaster("local[*]").setAppName("pathFinding")
    val sc = new SparkContext(conf)
    val input: RDD[String] = sc.textFile("src/main/resources/web-Google.txt", 5)
    val input2 = input.map(e => e)
    val input3 = input.map(e => e)
    sc.setCheckpointDir("C:\\Users\\zm\\Desktop\\lovainoutput\\hellospark")
    //    input.checkpoint()
    println(sc.getPersistentRDDs)
    println(input3.count())
    println(input.isCheckpointed)
    println(input3.dependencies.length)












    //    sc.getRDDStorageInfo.foreach(e => println(e.name))


    //    sc.getRDDStorageInfo
    //    input.take(10).foreach(println(_))
    //    input.partitions.foreach(e=>println(e.index))
    //    input.takeSample(true, 10).foreach(println(_))
    //    input.sample(true,100).countByValue()
    //    println(input.glom().count())
    //    input.retag
    //    input.mapPartitionsWithIndex((index, it) => it)
        input.collect()
    //    val haha: RDD[(Int, Int, Int)] = sc.parallelize(Array((1, 2, 5)))
    //    haha.map { case (a, b, uu) => 99 }
    //    input.partitions.foreach(e => println(e.index))
    //    println(input.map((_)).dependencies.length)


    //    val hellll = input.map((_, 1))
    //    val yy = hellll.map(e=>e)
    //    println(yy.dependencies.length)


  }

}
