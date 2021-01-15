package test3

import org.apache.spark.sql.SparkSession

object Test9 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("tt").config("spark.testing.memory", "2147480000").master("local[2]").getOrCreate()
    import ss.implicits._
    ss.sparkContext.parallelize(Array(
      ("A", "dev", "jack", 1, 100),
      ("A", "dev", "tom", 1, 200),
      ("A", "dev", "json", 0, 101),
      ("A", "dev", "garry", 0, 379),
      ("A", "market", "kobe", 1, 202),
      ("A", "market", "wade", 0, 300),
      ("A", "market", "lebron", 1, 320),
      ("A", "sale", "ann", 0, 222),
      ("A", "sale", "elmer", 0, 789),
      ("A", "sale", "thomas", 0, 320),
      ("A", "manager", "tony", 1, 333),
      ("A", "manager", "henry", 1, 390),
      ("B", "dev", "fred", 0, 367),
      ("B", "dev", "Ursa", 1, 329),
      ("B", "dev", "brian", 0, 422),
      ("B", "sale", "todd", 0, 321),
      ("B", "sale", "Crystal", 1, 333),
      ("B", "manager", "Emily", 1, 367),
      ("B", "manager", "Tracy", 0, 556)
    )).toDF("corp", "dept", "sex", "name", "salary").createTempView("t1")
    ss.sql(
      """
        |select
        |corp,
        |dept,
        |sex,
        |name,
        |-- grouping(corp),
        |-- grouping(dept),
        |-- grouping(sex),
        |-- grouping(name),
        |sum(salary)
        |from t1
        |--GROUP BY corp,dept,sex,name with rollup
        |GROUP BY rollup(corp,dept) ,rollup(sex,name)
      """.stripMargin).show(100)

  }

}
