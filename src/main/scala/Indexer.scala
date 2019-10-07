import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import utils.Functions
import org.apache.spark.sql.functions.{udf, _}


object Indexer {

  val usage = """
  Usages:
    Indexer input_path [output_path]        Index the files in input_path and store in output_path
    Indexer [-h]                            Print this help message
    """
  var inputPath = ""
  var outputPath = "/tmp"

  def handleArgs(args: Array[String]) = {
    if (args.length == 0){
      println(usage)
      System.exit(0)
    } else if (args(0)=="-h") {
      println(usage)
      System.exit(0)
    }
    inputPath = args(0)
    if (args.length == 2){
      outputPath = args(1)
    }
  }

  val logger: Logger = Logger.getLogger("org.apache.spark")
  logger.setLevel(Level.ERROR)

  def log(s: Any): Unit ={
    logger.fatal("BIGDATA:: " + s)
    println("BIGDATA:: " + s)
  }

  def main(args: Array[String]): Unit = {
    // handle command-line arguments
    handleArgs(args)
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

    log("creating word_to_idf")
    val word_to_idf = doc1DF.select(explode('word_freq) as Seq("word", "freq")).
      groupBy('word).
      count.
      withColumnRenamed("count", "idf")

    log("adding ids; creating word_to_id_idf")
    val word_to_id_idf = word_to_idf.withColumn("id", monotonically_increasing_id)

    // save word_to_id_idf
    word_to_id_idf.write.mode("overwrite").parquet(s"$outputPath/word_id")

    log("creating word_to_id_idf_map")
    // word => (id, idf)
    val word_to_id_idf_map = Functions.word_id_idf_collectAsMap(word_to_id_idf)

    log("creating word_to_id_idf_map")
    val freq_to_tfidf = udf(
      Functions.normalize_by_idf_and_encode_with_id(word_to_id_idf_map)(_: Map[String, Long])
    )

    log("creating doc2DF")
    // adds column tf/idf
    val doc2DF = doc1DF.withColumn(
      "vector",
      freq_to_tfidf.apply(doc1DF("word_freq"))
    )

    log("creating doc_index")
    val doc_index = doc2DF.select("id", "title", "vector")

    log("writing doc_index")
    // save doc_index
    doc_index.write.mode("overwrite").parquet(s"$outputPath/doc_index")

    log("writing final results to consolde")
    println("\n\n\n----------------------------------------------------------------------")
    println("Number of words: " + word_to_id_idf_map.size)
    println("Number of documents: " + doc_index.count())
    println("Indexing done!")
  }
}
