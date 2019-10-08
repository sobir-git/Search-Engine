package utils

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.CollectionAccumulator


/**
 * This object contains helper-functions
 */
object Functions {
  val logger: Logger = Logger.getLogger("org.apache.spark")

  /**
   * This function loads word-to-id Map from a path
   *
   * @param wordsPath : String - Path to the "words" directory of the index
   * @return
   */
  def load_word_to_id(wordsPath: String): collection.Map[String, Long] = {
    val sc = SparkSession.active.sparkContext
    val rdd = sc.sequenceFile[String, Long](s"$wordsPath/word_to_id")
    rdd.collectAsMap()
  }

  /**
   * This function loads wordId-to-id Map from a path
   *
   * @param wordsPath : String - Path to the "words" directory of the index
   * @return
   */
  def load_id_to_idf(wordsPath: String): collection.Map[Long, Int] = {
    val sc = SparkSession.active.sparkContext
    val rdd = sc.sequenceFile[Long, Int](s"$wordsPath/id_to_idf")
    rdd.collectAsMap()
  }


  /**
   * Removes the punctuation characters and converts to lower case
   *
   * @param word : String - word to be normalized
   * @return
   */
  def normalize_word(word: String): String = {
    word.toLowerCase.replaceAll("""[\p{Punct}]""", "")
  }

  /**
   * Convert a text to a word -> freq map
   * It splits the text into words and computes the frequency of each word
   *
   * @param text : String - text to be operated on
   * @return Map(word->freq) - A map of words to their frequencies in the text
   */
  def to_word_freq_map(text: String): Map[String, Int] = {
    if (text == null) {
      println("BIGDATA:: What the hell?")
      return Map()
    }
    text.split("""\s+""").map(word => normalize_word(word))
      .groupBy(identity).mapValues(arr => arr.length)
  }

  /**
   * Converts text into final vector representation
   * It encodes the words with ids and maps them to their frequencies
   *
   * @param text       : String - text to be represented in vector form
   * @param word_to_id : Map(word->id) - a map that maps words to their ids
   * @return Map(word id -> frequency) - A map that maps word ids to their frequencies
   */
  def vectorize_text(text: String, word_to_id: scala.collection.Map[String, Long]): Map[Long, Int] = {
    // convert text to map of word_id->freq
    val word_to_freq = to_word_freq_map(text)
    word_to_freq.map {
      case (word, freq) => word_to_id.getOrElse(word, -1L) -> freq
    }
  }
}
