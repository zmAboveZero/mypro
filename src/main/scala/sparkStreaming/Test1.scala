package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.CommonUtils


object Test1 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[2]") //这里指在本地运行，2个线程，一个监听，一个处理数据
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(20)) // 时间划分为20秒
    println(CommonUtils.getResourcesPath() + "testData\\inputFile")
    val lines = ssc.textFileStream(CommonUtils.getResourcesPath() + "testData")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

