package in.iitpkd.scala
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLDataSet {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use SparkSession Interface
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val schemaPeople = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/friends.csv")
      .as[Person]

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")

    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <=19")

    val results = teenagers.collect()

    results.foreach(println)

    spark.stop()
  }

}
