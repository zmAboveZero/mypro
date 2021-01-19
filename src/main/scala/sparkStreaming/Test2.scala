package sparkStreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Streaming_WordCountTest").setMaster("local[4]").set("spark.driver.host", "localhost")
    //获取SparkContext
    val sparkContext = new SparkContext(sparkConf)
    //设置日志级别
    sparkContext.setLogLevel("WARN")

    //获取StreamingContext  需要两个参数 SparkContext和duration，后者就是间隔时间
    val streamContext: StreamingContext = new StreamingContext(sparkContext, Seconds(5))

    //从socket获取数据
    val stream= streamContext.socketTextStream("localhost", 9999)

    //对数据进行计数操作
    val result = stream.flatMap(x => x.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //输出数据
    result.print()

    //启动程序
    streamContext.start()
    streamContext.awaitTermination()
  }

}
