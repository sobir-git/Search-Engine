import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import org.apache.log4j.{Level, Logger}


object Indexer {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("searchEngineIndexer")

    val spark = SparkSession.builder
      .appName("SparkSessionExample")
      .config(conf)
      .getOrCreate
    import spark.implicits._
    val sc = spark.sparkContext
    val logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.ERROR)


//    val inputPath = """src/main/resources/example-wiki""" //args(0)
    val inputPath = args(0)

    val textFile = sc.textFile(inputPath)
    val json_df = spark.read.json(textFile)

    val doc_words = json_df.map(row => (row.getAs("id").toString.toInt, row.getAs("text").toString.split(" "))).map { case (id, wordarr) => (id, wordarr.map(word => word.toLowerCase.replaceAll("""[\p{Punct}]""", ""))) }


    // map[unique word -> count] for each doc
    val doc_word_to_counts = doc_words
      .map { case (id, wordarr) => (id, wordarr.groupBy(identity).mapValues(arr => arr.length)) }

    val word_to_id_idf = doc_word_to_counts
      .flatMap(t => t._2.transform((word, cnt) => 1))
      .rdd
      .reduceByKey(_ + _)
      .sortBy(_._1)
      .zipWithIndex
      .map { case ((word, idf), id) => (id, word, idf) }
      .toDF("id", "word", "idf")

    // save word_to_id
    word_to_id_idf.write.mode("overwrite").parquet("tmp/word_id")

    // word => (id, idf)
    val word_to_id_idf_map = word_to_id_idf.rdd.map { row => (row(1).toString, (row(0).toString.toInt, row(2).toString.toInt)) }.collectAsMap
    logger.error("Number of words: " + word_to_id_idf.count())


    // doc_id => [(word_id -> tf/idf)]
    val doc_index = doc_word_to_counts.
      map { case (doc_id, wordmap)
      => (doc_id, wordmap.map { case (word, tf)
      => word_to_id_idf_map(word)._1 -> tf.toDouble / word_to_id_idf_map(word)._2
      })
      }.
      withColumnRenamed("_1", "doc_id").
      withColumnRenamed("_2", "wordmap")

    logger.error("Number of documents: " + doc_index.count() )

    // save doc_index
    doc_index.write.mode("overwrite").parquet("tmp/doc_index")
  }

}