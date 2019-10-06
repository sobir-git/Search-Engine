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

  def main(args: Array[String]): Unit = {
    // handle command-line arguments
    handleArgs(args)
    val spark = SparkSession.builder.
      getOrCreate()

    import spark.implicits._
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // has columns id, text, title, url
    val doc0DF = spark.read.json(inputPath)

    // added map(word->freq) as a column "word_freq"
    val doc1DF = doc0DF.withColumn("word_freq",
      udf(Functions.to_word_freq_map _).apply(doc0DF("text")))

    val word_to_idf = doc1DF.select(explode('word_freq) as Seq("word", "freq")).
      groupBy('word).
      count.
      withColumnRenamed("count", "idf")

    val word_to_id_idf = word_to_idf.withColumn("id", monotonically_increasing_id)

    // save word_to_id_idf
    word_to_id_idf.cache()
    word_to_id_idf.write.mode("overwrite").parquet(s"$outputPath/word_id")

    // word => (id, idf)
    val word_to_id_idf_map = Functions.word_id_idf_collectAsMap(word_to_id_idf)


    val freq_to_tfidf = udf(
      Functions.normalize_by_idf_and_encode_with_id(word_to_id_idf_map)(_: Map[String, Long])
    )

    // adds column tf/idf
    val doc2DF = doc1DF.withColumn(
      "vector",
      freq_to_tfidf.apply(doc1DF("word_freq"))
    )

    val doc_index = doc2DF.select("id", "title", "vector")

    // save doc_index
    doc_index.write.mode("overwrite").parquet(s"$outputPath/doc_index")

    println("\n\n\n----------------------------------------------------------------------")
    println("Number of words: " + word_to_id_idf.count())
    println("Number of documents: " + doc_index.count())
    println("Indexing done!")
    System.in.read
    spark.stop
  }
}
