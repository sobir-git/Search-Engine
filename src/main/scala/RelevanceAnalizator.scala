import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{desc, udf}

object RelevanceAnalizator {

  def computeDotProduct(map1: Map[Long, Double])(map2: Map[Long, Double]): Double = {
    map1.keySet.intersect(map2.keySet).map(k => map1(k) * map2(k)).sum
  }

  def computeRelevance(vectorized_query: Map[Long, Double], doc_index: DataFrame, topN: Int = 10): DataFrame = {
    val spark = SparkSession.active
    import spark.implicits._

    doc_index.withColumn("relevance",
      udf(computeDotProduct(vectorized_query)(_: Map[Long, Double])).
        apply('vector)
    )
  }

  def getTopRelevances(relevances: DataFrame, topN: Int): Array[Row] = {
    relevances
      .orderBy(desc("relevance"))
      .take(topN)
  }
}
