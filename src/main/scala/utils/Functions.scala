package utils

import scala.collection.JavaConverters._

object Functions {
  def normalize_word(word: String): String = {
    word.toLowerCase.replaceAll("""[\p{Punct}]""", "")
  }
  def to_word_freq_map(text: String): Map[String, Int] = {
    text.split(" ").map(word => normalize_word(word))
      .groupBy(identity).mapValues(arr => arr.length)
  }

  def normalize_by_idf_and_encode_with_id(word_to_freq: Map[String, Int], word_to_id_idf: scala.collection.Map[String, (Int, Int)]) = {
    word_to_freq.map {
      case (word, freq)
      => word_to_id_idf.getOrElse(word, (-1, 0))._1 -> freq.toDouble / word_to_id_idf(word)._2
    }
  }

  def vectorize_text(text: String, word_to_id_idf: scala.collection.Map[String, (Int, Int)]) = {
    val word_to_freq_map = to_word_freq_map(text)
    normalize_by_idf_and_encode_with_id(word_to_freq_map, word_to_id_idf)
  }
}
