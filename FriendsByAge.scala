package in.iitpkd.scala
import org.apache.spark._
import org.apache.log4j._

object FriendsByAge {

  def parseLine(line: String) = {
    //split by commas
    val fields = line.split(",")

    val age = fields(2).toInt
    val numFriends = fields(3).toInt

    //Create tuple
    (age, numFriends)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","FriendsByAge")

    val lines = sc.textFile("data/friends-noheader.csv")

    // using parseLine function to convert into required tuples
    val rdd = lines.map(parseLine)

    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    // Collect results from RDD
    val results = averagesByAge.collect()

    results.sorted.foreach(println)
  }
}
