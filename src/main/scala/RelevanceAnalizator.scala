import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object RelevanceAnalizator {

  def computeDotProduct(map1: Map[Int, Double], map2: Map[Int, Double]) = {
    map1.keySet.intersect(map2.keySet).map(k => map1(k) * map2(k)).sum
  }

  def computeRelevance(vectorized_query: Map[Int, Double], document_index: DataFrame, topN: Int = 10): Dataset[(Int, Double)] = {
    val spark = SparkSession.active
    import spark.implicits._

    val relevances = document_index
      .map(
        row => (
          row.getAs("doc_id").asInstanceOf[Int],
          computeDotProduct(
            vectorized_query,
            row.getAs("wordmap").asInstanceOf[Map[Int, Double]]
          )
        )
      )
    relevances
  }

  def getTopRelevances(relevances: Dataset[(Int, Double)], topN: Int) = {
    relevances
      .rdd
      .top(topN)(Ordering[Double].on(_._2))
  }
}
