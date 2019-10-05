import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import utils.Functions
import org.apache.spark.sql.functions.{udf, _}


object Indexer {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("searchEngineIndexer")

    val spark = SparkSession.builder.
      config(conf).
      getOrCreate()

    import spark.implicits._
    val logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.ERROR)


    //    val inputPath = """src/main/resources/example-wiki""" //args(0)
    val inputPath = args(0)

    // has columns id, text, title, url
    val doc0DF = spark.read.json(inputPath)

    // added map(word->freq) as a column "word_freq"
    val doc1DF = doc0DF.withColumn("word_freq",
      udf(Functions.to_word_freq_map _).apply(doc0DF("text")))

    val word_to_idf = doc1DF.select(explode('word_freq) as Seq("word", "freq")).groupBy('word).count.withColumnRenamed("count", "idf")
    val word_to_id_idf = word_to_idf.withColumn("id", monotonically_increasing_id)


    // save word_to_id
    word_to_id_idf.write.mode("overwrite").parquet("tmp/word_id")

    // word => (id, idf)
    val word_to_id_idf_map = Functions.word_id_idf_collectAsMap(word_to_id_idf)

    logger.error("Number of words: " + word_to_id_idf.count())

    val freq_to_tfidf = udf(
      Functions.normalize_by_idf_and_encode_with_id(word_to_id_idf_map)(_: Map[String, Long])
    )

    // adds column tf/idf
    val doc2DF = doc1DF.withColumn(
      "vector",
      freq_to_tfidf.apply(doc1DF("word_freq"))
    )

    val doc_index = doc2DF.select("id", "title", "vector")
    logger.error("Number of documents: " + doc_index.count())

    // save doc_index
    doc_index.write.mode("overwrite").parquet("tmp/doc_index")
  }
}
