import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import utils.Functions
import org.apache.spark.sql.functions.avg

import scala.collection.mutable


/**
 * This object holds the logic of Ranker
 * It uses RelevanceAnalizator functions to compute ranks
 *
 */
object Ranker {

  val usage =
    """
    Usage:
      Ranker [options] rank_function                Run Ranker with rank_function
      Ranker [options] rank_function [queries*]     Search for query using rank_function
      Ranker -h                                          Print this help message

    Where:
      rank_function     currently only "inner"  ("BM25" in the future;))
      [queries*]             query string

    Options:
      -i index_path          Index path - the output path of Indexer

    Examples:
      Ranker -i IndexPath inner
      Ranker -i IndexPath inner Differential Geometry
      Ranker -i IndexPath inner Differential Geometry
    """

  var indexPath = "/tmp"
  var rank_function: String = _
  var search_query: String = _
  var average_document_length: Double = _
  var doc_count: Int = _

  /**
   * Interpret command-line arguments
   *
   * @param args - command line arguments
   */
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

    rank_function = args(0).toLowerCase
    if (!Seq("inner", "bm25").contains(rank_function)) {
      println(s"Invalid ranking function: $rank_function")
      println("It has to be either 'inner' or 'bm25'")
      System.exit(1)
    }

    search_query = args.splitAt(1)._2.mkString(" ")
  }

  /**
   * Display the results of query
   *
   * @param top_documents - top ranked documents
   */
  def displayResults(top_documents: Array[Row]): Unit = {
    // print top ranks (doc_id, rel)
    println(s"\n\nQuery results for: $search_query")
    println("%1$10s %2$-60s %3$-10s ".format("DocID", "Title", "Rank"))

    top_documents foreach {
      row =>
        println(f"${row.getAs("id")}%10s" +
          f" ${row.getAs("title")}%-60s" +
          f" ${row.getAs[Double]("rank")}%.6f")
    }
  }


  def main(args: Array[String]): Unit = {
    val bargs = args.toBuffer
    interpretArgs(bargs)

    val spark = SparkSession.builder.getOrCreate()
    //    import spark.implicits._  in case of emergency
    val sc = spark.sparkContext
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // read document index,   doc_id, Map(word->tf)
    val doc_index = spark.read.parquet(s"$indexPath/doc_index")
    doc_count = doc_index.count().toInt
    println(s"document count: $doc_count")

    average_document_length = doc_index.select(avg("length")).head.getAs[Double](0)
    println(s"average_document_length: $average_document_length")

    //load Map[word->id] / will be used to vectorize text
    val word_to_id = Functions.load_word_to_id(s"$indexPath/words")
    println(s"word to id :  ${word_to_id.take(30)}")

    val id_to_idf = Functions.load_id_to_idf(s"$indexPath/words")

    println(s"id to idf :  ${id_to_idf.take(30)}")

    while (true) {
      while (search_query == "") {
        println("\n\n------------------------------------------------------------")
        search_query = scala.io.StdIn.readLine("Enter a search query: ")
      }

      // vectorize query
      val vectorized_query = Functions.vectorize_text(search_query, word_to_id)
      println(s"vectorized_query = $vectorized_query")

      // compute relevances with each document
      var doc_index_with_relevances: DataFrame = null
      if (rank_function == "inner") {
        doc_index_with_relevances = RelevanceAnalizator.computeRankInnerProduct(
          vectorized_query = vectorized_query,
          doc_index = doc_index,
          id_to_idf = id_to_idf
        )
      } else if (rank_function == "bm25") {
        doc_index_with_relevances = RelevanceAnalizator.computeRankBM25(
          vectorized_query = vectorized_query,
          doc_index = doc_index,
          id_to_idf = id_to_idf,
          avgdl = average_document_length,
          doc_count = doc_count
        )
      }


      // get top 20 relevant documents
      val top_documents = RelevanceAnalizator.getTopRanked(doc_index_with_relevances, topN = 20)

      // display results in console
      displayResults(top_documents)

      // reset query
      search_query = ""
    }
  }
}
