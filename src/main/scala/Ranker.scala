import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import utils.Functions

import scala.collection.mutable


object Ranker {

  val usage =
    """
    Usage:
      Ranker [options] relevance_function                Run Ranker with relevance_function
      Ranker [options] relevance_function [queries*]     Search for query using relevance_function
      Ranker -h                                          Print this help message

    Where:
      relevance_function     currently only "inner"  ("BM25" in the future;))
      [queries*]             query string

    Options:
      -i index_path          Index path - the output path of Indexer

    Examples:
      Ranker inner
      Ranker inner World War II
    """

  var indexPath = "/tmp"
  var relevance_function = "inner"
  var search_query = ""

  def interpretArgs(args: mutable.Buffer[String]): Unit = {

    if (args.isEmpty) {
      println(usage)
      System.exit(0)
    } else if (args.head == "-h") {
      println(usage)
      System.exit(0)
    }

    if (args.head == "-i") {
      args.remove(0)
      indexPath = args.head
      args.remove(0)
    }

    relevance_function = args(0).toLowerCase
    if (relevance_function == "inner") {
    } else if (relevance_function == "bm25") {
      println("bm25 is not supported yet, relying back on \"inner\"")
    }
    else {
      println(s"invalid relevance function: $relevance_function")
      System.exit(1)
    }

    search_query = args.splitAt(1)._2.mkString(" ")
  }

  def displayResults(top_documents: Array[Row]): Unit = {
    // print top relevances (doc_id, rel)
    println(s"\n\nQuery results for: $search_query")
    println("%1$10s %2$-60s %3$-10s ".format("DocID", "Title", "Relevance"))

    top_documents foreach {
      row =>
        println(f"${row.getAs("id")}%10s" +
          f" ${row.getAs("title")}%-60s" +
          f" ${row.getAs[Double]("relevance")}%.6f")
    }
  }

  def main(args: Array[String]): Unit = {
    val bargs = args.toBuffer
    interpretArgs(bargs)

    val spark = SparkSession.builder.getOrCreate()
    //    import spark.implicits._  in case of emergency
    val sc = spark.sparkContext
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // read document index,   doc_id, Map(word->tf/idf)
    val doc_index = spark.read.parquet(s"$indexPath/doc_index")

    //load Map[word->(id, idf)] / will be used to vectorize text
    val word_to_id_idf_map = Functions.load_word_to_id_idf_map(indexPath)

    while (true) {
      while (search_query == "") {
        println("\n\n------------------------------------------------------------")
        search_query = scala.io.StdIn.readLine("Enter a search query: ")
      }

      // vectorize query
      val vectorized_query = Functions.vectorize_text(search_query, word_to_id_idf_map)

      // compute relevances with each document
      val doc_index_with_relevances = RelevanceAnalizator.computeRelevance(vectorized_query, doc_index)

      // get top 10  / Array [(Int, Double)]
      val top_documents = RelevanceAnalizator.getTopRelevances(doc_index_with_relevances, topN = 20)

      // display results in console
      displayResults(top_documents)

      // reset query
      search_query = ""
    }
  }
}
