import org.apache.spark.sql.catalyst.expressions.Length
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{desc, udf}

/**
 * Relevance Analizator
 * This object containes functions to calculate query-document relevances
 * It implements two ranking functions
 * 1) simple Inner Product
 * 2) BM25
 */
object RelevanceAnalizator {

  /**
   * Compute relevances using inner product
   *
   * @param vectorized_query - query in sparse vector form (Map(word->freq))
   * @param doc_index        - dataframe containing document text in sparse vector (Map(word->freq))
   * @param id_to_idf        - A map that maps word ids to IDFs
   * @return doc_index with added "rank" column
   */
  def computeRankInnerProduct(vectorized_query: Map[Long, Int], doc_index: DataFrame, id_to_idf: scala.collection.Map[Long, Int]): DataFrame = {
    val spark = SparkSession.active
    import spark.implicits._

    /**
     * Inner product function to compute rank between document and query
     *
     * @param doc_vec : Map(word ID -> freq) - sparse vector representation of the documents
     * @return Number - The relevance quantity
     */
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

    doc_index.withColumn("rank",
      udf(inner _).apply('id_freq)
    )
  }

  /**
   * Compute relevances using BM25 ranking function
   *
   * @param vectorized_query - query in sparse vector form (Map(word->freq))
   * @param doc_index        - dataframe containing document text in sparse vector (Map(word->freq))
   * @param id_to_idf        - A map that maps word ids to IDFs
   * @param avgdl            - Average document length
   * @param doc_count        - Total number of documents
   * @return doc_index with added "rank" column
   */
  def computeRankBM25(vectorized_query: Map[Long, Int],
                      doc_index: DataFrame,
                      id_to_idf: scala.collection.Map[Long, Int],
                      avgdl: Double, doc_count: Int): DataFrame = {
    val spark = SparkSession.active
    import spark.implicits._


    /**
     * BM25 function to compute rank between a document and query
     *
     * @param doc_vec : Map(word ID -> freq) - sparse vector representation of the documents
     * @param length  : Int - length of document
     * @return Number - The rank quantity
     */
    def inner(doc_vec: Map[Long, Int], length: Int): Double = {
      val k1 = 1.5d
      val b = 0.75d
      vectorized_query.keySet.intersect(doc_vec.keySet).
        map(
          word_id => {
            val nq = id_to_idf(word_id).toDouble
            val idf = math.log(doc_count / nq)
            val word_doc_freq = doc_vec(word_id)
            val word_query_freq = vectorized_query(word_id)
            println(s"nq = $nq")
            println(s"idf = $idf")
            println(s"word_doc_freq = $word_doc_freq")
            println(s"word_query_freq = $word_query_freq")
            val score = word_query_freq *
              idf *
              (word_doc_freq * (k1 + 1)) /
              (word_doc_freq + k1 * (1 - b + b * length / avgdl))
            score
          }
        ).sum
    }

    doc_index.withColumn("rank",
      udf(inner _).apply('id_freq, 'length)
    )
  }

  /**
   * Get top N relevant documents from dataframe
   * Note that the "rank" column of dataframe should already be computed
   *
   * @param doc_index_with_relevance : DataFrame - document index with pre-computed ranks
   * @param topN                     : Int - Number of rows to retrieve from top
   * @return: Array of Row - The topN most relevant documents
   */
  def getTopRanked(doc_index_with_relevance: DataFrame, topN: Int): Array[Row] = {
    doc_index_with_relevance
      .orderBy(desc("rank"))
      .take(topN)
  }
}
