import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.log4j.{Level, Logger}
import utils.Functions


object Ranker {
  def main(args: Array[String]): Unit = {
    val search_query = args(0)

    val conf = new SparkConf().setAppName("searchEngineIndexer")

    val spark = SparkSession.builder
      .appName("SparkSessionExample")
      .master("local")
      .config(conf)
      .getOrCreate
//    import spark.implicits._  in case of emergency
    val sc = spark.sparkContext
    val logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.ERROR)

    // read document index,   doc_id, Map(word->tf/idf)
    val doc_index = spark.read.parquet("tmp/doc_index")

    //load Map[word->(id, idf)] / will be used to vectorize text
    val word_to_id_idf_map = Functions.load_word_to_id_idf_map()

    // vectorize query
    val vectorized_query = Functions.vectorize_text(search_query, word_to_id_idf_map)

    // compute relevances with each document
    val doc_index_with_relevances = RelevanceAnalizator.computeRelevance(vectorized_query, doc_index)


    // get top 10  / Array [(Int, Double)]
    val top_documents = RelevanceAnalizator.getTopRelevances(doc_index_with_relevances, topN = 20)

    // print top relevances (doc_id, rel)
    println("%1$10s %2$-60s %3$-10s ".format("DocID", "Title", "Relevance"))

    top_documents foreach {
      row =>
        println(f"${row.getAs("id")}%10s" +
          f" ${row.getAs("title")}%-60s" +
          f" ${row.getAs[Double]("relevance")}%.6f")
    }
  }
}
