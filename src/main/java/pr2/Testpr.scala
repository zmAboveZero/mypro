package pr2

object Testpr {
  def main(args: Array[String]): Unit = {

    val list = List(("1", "2"), ("2", "3"), ("3", "4"), ("4", "5"), ("1", "3"), ("1", "5"), ("5", "3"), ("3", "6"), ("1", "6"))

    var links = List(("A", List("D")), ("B", List("A")), ("C", List("A", "B")), ("D", List("A", "C")))
    var ranks = List(("A", 1.0), ("B", 1.0), ("C", 1.0), ("D", 1.0))

    //    val tuples = links.zip(ranks).map(ele => (ele._1._1, (ele._1._2, ele._2._2))).flatMap { case (url, (links, rank)) => links.map(dest => (dest, rank / links.size)) }
    //    tuples.groupBy(_._1).map(ele => (ele._1, ele._2.map(_._2).sum)).toList.map(ele => (ele._1, 0.15 + 0.85 * ele._2)).foreach(println(_))
    for (i <- 1 to 200) {
      val c = links.zip(ranks).map(ele => (ele._1._1, (ele._1._2, ele._2._2))).flatMap { case (url, (links, rank)) => links.map(dest => (dest, rank / links.size)) }
      ranks = c.groupBy(_._1).map(ele => (ele._1, ele._2.map(_._2).sum)).toList.map(ele => (ele._1, 0.15 + 0.85 * ele._2))
    }

    ranks.sortBy(_._2).reverse.foreach(println(_))
//    ranks.foreach(println(_))


  }

}
