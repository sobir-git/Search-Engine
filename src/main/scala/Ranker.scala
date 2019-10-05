import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import org.apache.log4j.{Level, Logger}


object Ranker {
  def main(args: Array[String]): Unit = {
    val search_query = args(0)

    val conf = new SparkConf().setAppName("searchEngineIndexer")

    val spark = SparkSession.builder
      .appName("SparkSessionExample")
      .config(conf)
      .getOrCreate
    import spark.implicits._
    val sc = spark.sparkContext
    val logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.ERROR)

    // read document index,   doc_id, Map(word->tf/idf)
    val doc_index = spark.read.parquet("tmp/doc_index")
    //read word_to_id
    val word_to_id_idf = spark.read.parquet("tmp/word_id")



  }
}
