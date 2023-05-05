package in.iitpkd.scala
import org.apache.spark._
import org.apache.log4j._

object RatingsCounter {

  def main(args: Array[String]): Unit = {

    // setting log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // create spark context using every core
    val sc = new SparkContext("local[*]","RatingsCounter")

    // load lines into RDD
    val lines = sc.textFile("ml-100k/u.data")

    // convert each line into string, split by tabs, and extract third field
    val ratings = lines.map(x => x.toString().split("\t")(2))

    // Count up each occurrence of a rating
    val results = ratings.countByValue()

    // Sort the resulting map (results,count)
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line
    sortedResults.foreach(println)
  }
}
