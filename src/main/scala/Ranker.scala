import org.apache.spark.{SparkConf}
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
    import spark.implicits._
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
    val relevances = RelevanceAnalizator.computeRelevance(vectorized_query, doc_index)

    // get top 10  / Array [(Int, Double)]
    val top_relevances = RelevanceAnalizator.getTopRelevances(relevances, topN = 10)

    // print top relevances (doc_id, rel)
    println("Doc_id\tRelevance")
    top_relevances foreach {
      case (doc_id, rel) => println(f"$doc_id  \t$rel%.4f")
    }

  }
}
