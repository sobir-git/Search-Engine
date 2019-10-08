import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import utils.Functions
import org.apache.spark.sql.functions.{udf, _}

import scala.collection.mutable


/**
 * Indexer
 * This contains the logic to run indexing
 *
 */
object Indexer {

  val usage =
    """
  Usages:
    Indexer [-w wordsPath] input_path [output_path]        Index the files in input_path and store in output_path
    Indexer [-h]                            Print this help message
    """
  var inputPath = ""
  var outputPath = "/tmp"
  var wordsPath = ""


  /**
   * Interpret command-line arguments
   *
   * @param args - command line arguments
   */
  def handleArgs(args: mutable.Buffer[String]) = {
    if (args.length == 0) {
      println(usage)
      System.exit(0)
    } else if (args(0) == "-h") {
      println(usage)
      System.exit(0)
    }

    if (args.head == "-w") {
      args.remove(0)
      wordsPath = args.head
      args.remove(0)
    }

    inputPath = args(0)
    if (args.length == 2) {
      outputPath = args(1)
    }
  }

  val logger: Logger = Logger.getLogger("org.apache.spark")
  logger.setLevel(Level.ERROR)

  def log(s: Any): Unit = {
    logger.fatal("BIGDATA:: " + s)
    println("BIGDATA:: " + s)
  }

  def main(args: Array[String]): Unit = {
    // handle command-line arguments
    handleArgs(args.toBuffer)
    val spark = SparkSession.builder.
      getOrCreate()

    import spark.implicits._

    // has columns id, text, title, url
    log("reading docDF")
    val doc0DF = spark.read.json(inputPath)

    log("reading doc1DF")
    // added map(word->freq) as a column "word_freq"
    val doc1DF = doc0DF.withColumn("word_freq",
      udf(Functions.to_word_freq_map _).apply(doc0DF("text")))

    var word_to_id: scala.collection.Map[String, Long] = Map("sobir" -> 22L)
    var id_to_idf: scala.collection.Map[Long, Int] = Map()
    if (wordsPath != "") {
      log(s"loading words from $wordsPath")
      word_to_id = Functions.load_word_to_id(wordsPath)
      id_to_idf = Functions.load_id_to_idf(wordsPath)
    } else {
      log("creating word_to_idf")
      val word_to_idf_RDD = doc1DF.select(explode('word_freq) as Seq("word", "freq")).
        groupBy('word).
        count.
        withColumnRenamed("count", "idf").
        as[(String, Long)].rdd

      val word_to_id_RDD = word_to_idf_RDD.keys.zipWithUniqueId()

      log("saving word_to_id_RDD")
      word_to_id_RDD.saveAsSequenceFile(s"$outputPath/words/word_to_id")

      log("creating word_to_id")
      word_to_id = word_to_id_RDD.collectAsMap()

      val id_to_idf_RDD = word_to_idf_RDD.map {
        case (word, idf) => word_to_id(word) -> idf.toInt
      }

      log("saving id_to_idfRDD")
      id_to_idf_RDD.saveAsSequenceFile(s"$outputPath/words/id_to_idf")

      log("creating id_to_idf")
      id_to_idf = id_to_idf_RDD.collectAsMap()

      println(s"id to idf:  ${id_to_idf.take(20)}")
    }

    log(s"word count: ${word_to_id.size}")

    def replace_word_with_id(word_freq: Map[String, Int]): Map[Long, Int] = {
      word_freq.map {
        case (word, freq) => (word_to_id(word), freq)
      }
    }

    // adds column tf/idf
    val doc2DF = doc1DF.withColumn(
      "id_freq",
      udf(replace_word_with_id _).apply(doc1DF("word_freq"))
    )

    // calculate document length
    val calc_doc_length = (id_freq: Map[Long, Int]) => {
      id_freq.values.sum
    }
    val doc3DF = doc2DF.withColumn(
      "length",
      udf(calc_doc_length).apply('id_freq)
    )

    log("creating doc_index")
    val doc_index = doc3DF.select("id", "title", "id_freq", "length")

    log("writing doc_index")
    // save doc_index
    doc_index.write.mode("overwrite").parquet(s"$outputPath/doc_index")

    log("showing doc_index")
    doc_index.show

    log("writing final results to console")
    println("\n\n\n----------------------------------------------------------------------")
    println("Number of words: " + word_to_id.size)
    println("Number of documents: " + doc_index.count())
    println("Indexing done!")
  }
}
