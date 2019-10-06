package utils

import org.apache.spark.sql.{DataFrame, SparkSession}


object Functions {
  def normalize_word(word: String): String = {
    word.toLowerCase.replaceAll("""[\p{Punct}]""", "")
  }

  def to_word_freq_map(text: String): Map[String, Long] = {
    text.split("""\s+""").map(word => normalize_word(word))
      .groupBy(identity).mapValues(arr => arr.length)
  }

  def normalize_by_idf_and_encode_with_id(word_to_id_idf: scala.collection.Map[String, (Long, Long)])(word_to_freq: Map[String, Long]): Map[Long, Double] = {
    word_to_freq.map {
      case (word, freq)
      => word_to_id_idf.getOrElse(word, (-1L, 0))._1 -> freq.toDouble / word_to_id_idf.getOrElse(word, (-1L, 1L))._2
    }
  }

  def vectorize_text(text: String, word_to_id_idf: scala.collection.Map[String, (Long, Long)]): Map[Long, Double] = {
    val word_to_freq_map = to_word_freq_map(text)
    normalize_by_idf_and_encode_with_id(word_to_id_idf)(word_to_freq_map)
  }

  def word_id_idf_collectAsMap(word_to_id_idf: DataFrame): collection.Map[String, (Long, Long)] = {
    val word_to_id_idf_map = word_to_id_idf.rdd.map {
      row =>
        (
          row.getAs("word").asInstanceOf[String],
          (row.getAs[Long]("id"), row.getAs[Long]("idf"))
        )
    }.collectAsMap
    word_to_id_idf_map
  }

  def load_word_to_id_idf_map(indexPath: String): collection.Map[String, (Long, Long)] = {
    val spark = SparkSession.active
    val word_to_id_idf = spark.read.parquet(s"$indexPath/word_id")
    val word_to_id_idf_map = word_id_idf_collectAsMap(word_to_id_idf)
    word_to_id_idf_map
  }
}
