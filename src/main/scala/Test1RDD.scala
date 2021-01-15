import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

class Test1RDD(input: Array[String], sc: SparkContext) extends RDD[String](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    split.asInstanceOf[Test1RDDPartition].con.map(_*10).toIterator
  }

  override protected def getPartitions: Array[Partition] = {
    val tuple = input.splitAt(2)
    Array(new Test1RDDPartition(tuple._1, 0), new Test1RDDPartition(tuple._2, 1))
  }
}

class Test1RDDPartition(par: Array[String], id: Int) extends Partition {
  val con = par

  override def index: Int = id
}