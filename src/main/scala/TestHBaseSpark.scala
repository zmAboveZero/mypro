import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.{HBaseContext, HBaseRDDFunctions, KeyFamilyQualifier}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._

object TestHBaseSpark {
  def main(args: Array[String]): Unit = {


    val Array(outputPath, tableName) = args
    val columnFamily1 = "f1"
    val columnFamily2 = "f2"

    val sparkConf = new SparkConf().setAppName("JavaHBaseBulkLoadExample " + tableName)
    val sc = new SparkContext(sparkConf)

    try {
      val arr = Array("1," + columnFamily1 + ",b,1",
        "2," + columnFamily1 + ",a,2",
        "3," + columnFamily1 + ",b,1",
        "3," + columnFamily2 + ",a,1",
        "4," + columnFamily2 + ",a,3",
        "5," + columnFamily2 + ",b,3")

      val rdd = sc.parallelize(arr)
      val config = HBaseConfiguration.create
      config.set("hfile.compression", Compression.Algorithm.SNAPPY.getName())
      val hbaseContext = new HBaseContext(sc, config)

      hbaseContext.hbaseRDD(TableName.valueOf(""), new Scan())
      //      rdd.hbaseForeachPartition
      //      rdd.hbaseForeachPartition()
      HBaseRDDFunctions


      hbaseContext.bulkLoad[String](rdd,
        TableName.valueOf(tableName),
        (putRecord) => {
          if (putRecord.length > 0) {
            val strArray = putRecord.split(",")
            val kfq = new KeyFamilyQualifier(Bytes.toBytes(strArray(0)), Bytes.toBytes(strArray(1)), Bytes.toBytes(strArray(2)))
            val pair = new Pair[KeyFamilyQualifier, Array[Byte]](kfq, Bytes.toBytes(strArray(3)))
            val ite = (kfq, Bytes.toBytes(strArray(3)))
            val itea = List(ite).iterator
            itea
          } else {
            null
          }
        },
        outputPath)
    } finally {
      sc.stop()
    }
  }

}
