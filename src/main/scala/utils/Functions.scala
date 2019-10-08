package utils

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.CollectionAccumulator


object Functions {
  def load_word_to_id(wordsPath: String): collection.Map[String, Long] = {
    val sc = SparkSession.active.sparkContext
    val rdd = sc.sequenceFile[String, Long](s"$wordsPath/word_to_id")
    rdd.collectAsMap()
  }

  def load_id_to_idf(wordsPath: String): collection.Map[Long, Int] = {
    val sc = SparkSession.active.sparkContext
    val rdd = sc.sequenceFile[Long, Int](s"$wordsPath/id_to_idf")
    rdd.collectAsMap()
  }

  val logger: Logger = Logger.getLogger("org.apache.spark")

  def normalize_word(word: String): String = {
    word.toLowerCase.replaceAll("""[\p{Punct}]""", "")
  }

  def to_word_freq_map(text: String): Map[String, Int] = {
    if (text == null) {
      println("BIGDATA:: What the hell?")
      return Map()
    }
    text.split("""\s+""").map(word => normalize_word(word))
      .groupBy(identity).mapValues(arr => arr.length)
  }

  def encode_with_word_id(word_to_id_idf: scala.collection.Map[String, (Long, Long)], logLines: CollectionAccumulator[String] = null)
                                         (word_to_freq: Map[String, Long]): Map[Long, Long] = {
    word_to_freq.map {
      case (word, freq) => {
        val (id, idf) = word_to_id_idf.getOrElse(word, (-1L, -1L))
        if (id == -1) {
          if (logLines != null) {
            logLines.add("BigDATA:: What the hell with this?")
            logLines.add(s"word: $word")
            logLines.add(s"word_to_freq: $word_to_freq")
            logLines.add(s"word_to_id_idf: $word_to_id_idf")
          }
        }
        id -> freq
      }
    }
  }

  def vectorize_text(text: String, word_to_id: scala.collection.Map[String, Long]): Map[Long, Int] = {
    // convert text to map of word_id->freq
    val word_to_freq = to_word_freq_map(text)
    word_to_freq.map{
      case (word, freq) => word_to_id.getOrElse(word, -1L) -> freq
    }
  }

  def word_idf_collect_as_map(word_to_idfDF: DataFrame): collection.Map[String, (Long, Long)] = {
    val spark = SparkSession.active
    import spark.implicits._
    val word_to_id_idf_map = word_to_idfDF.as[(String, Long)].rdd.zipWithUniqueId().map{
      case ((word, idf), id) => word -> (id, idf)
    }.collectAsMap()
    word_to_id_idf_map
  }

  def load_word_to_id_idf_map(wordsPath: String): collection.Map[String, (Long, Long)] = {
    val spark = SparkSession.active
    val word_to_id_idf = spark.read.parquet(wordsPath)
    val word_to_id_idf_map = word_idf_collect_as_map(word_to_id_idf)
    word_to_id_idf_map
  }
}
