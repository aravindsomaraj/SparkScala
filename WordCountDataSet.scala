package in.iitpkd.scala

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountDataSet {

  case class Book(value:String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // Read each line of my book into a Dataset
    import spark.implicits._
    val input = spark.read.text("data/The_Hunger_Games.txt").as[Book]

    // Split using regular expression to extract words
    val words = input
      .select(explode(split($"value","\\W+")).alias("word"))
      .filter($"word" =!= "")

    // Normalize all to lowercase
    val lowercaseWords = words.select(lower($"word").alias("word"))

    // Count up occurences of each word
    val wordCounts = lowercaseWords.groupBy("word").count()

    //Sorted by counts
    val wcSorted = wordCounts.sort("count")

    // Show results
    wcSorted.show(wcSorted.count.toInt)

    spark.stop()
  }
}
