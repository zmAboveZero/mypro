import org.apache.spark.sql.SparkSession

object Test2 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
    //    ss.sparkContext. setLogLevel( "ERROR" )
    import ss.implicits._
    ss.sparkContext.parallelize(List(1, 2, 3, 4, 5)).toDF("f").show()
    println("zhaomeng")
    println("zhaomeng")
    println("zhaomeng")

  }

}
