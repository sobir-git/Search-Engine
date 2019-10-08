import org.apache.spark.sql.catalyst.expressions.Length
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{desc, udf}

object RelevanceAnalizator {

  def computeDotProduct(map1: Map[Long, Double])(map2: Map[Long, Double]): Double = {
    map1.keySet.intersect(map2.keySet).map(k => map1(k) * map2(k)).sum
  }

  def computeRelevanceInnerProduct(vectorized_query: Map[Long, Int], doc_index: DataFrame, id_to_idf: scala.collection.Map[Long, Int], topN: Int = 10): DataFrame = {
    val spark = SparkSession.active
    import spark.implicits._

    def inner(doc_vec: Map[Long, Int]): Double = {
      println(s"$vectorized_query, $doc_vec")
      vectorized_query.keySet.intersect(doc_vec.keySet).
        map(
          word_id => {
            val idf = id_to_idf(word_id).toDouble
            val result = (vectorized_query(word_id) / idf) * (doc_vec(word_id) / idf)
            println(s"idf = $idf")
            println(s"$word_id => $result")
            result
          }
        ).sum
    }

    doc_index.withColumn("relevance",
      udf(inner _).apply('id_freq)
    )
  }

  def computeRelevanceBM25(vectorized_query: Map[Long, Int],
                           doc_index: DataFrame,
                           id_to_idf: scala.collection.Map[Long, Int],
                           avgdl:Double, doc_count:Int,
                           topN: Int = 10): DataFrame =
  {
    val spark = SparkSession.active
    import spark.implicits._

    def inner(doc_vec: Map[Long, Int], length: Int): Double = {
      val k1 = 1.5d
      val b = 0.75d
      vectorized_query.keySet.intersect(doc_vec.keySet).
        map(
          word_id => {
            val nq = id_to_idf(word_id).toDouble
//            val idf = max(0, math.log( (doc_count-nq+0.5)/(nq+0.5) ))   // alternative way in wikipedia
            val idf = math.log( doc_count/nq )
            val word_doc_freq = doc_vec(word_id)
            val word_query_freq = vectorized_query(word_id)
            println(s"nq = $nq")
            println(s"idf = $idf")
            println(s"word_doc_freq = $word_doc_freq")
            println(s"word_query_freq = $word_query_freq")
            val score = word_query_freq *
              idf *
              (word_doc_freq * (k1+1)) /
              (word_doc_freq+k1*(1-b+b*length/avgdl))
            score
          }
        ).sum
    }

    doc_index.withColumn("relevance",
      udf(inner _).apply('id_freq, 'length)
    )
  }

  def getTopRelevances(relevances: DataFrame, topN: Int): Array[Row] = {
    relevances
      .orderBy(desc("relevance"))
      .take(topN)
  }
}
