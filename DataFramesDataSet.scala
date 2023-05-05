package in.iitpkd.scala
import org.apache.spark.sql._
import org.apache.log4j._

object DataFramesDataSet {

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
    val people = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/friends.csv")
      .as[Person]

    println("Here's schema")
    people.printSchema()

    println("Select name col")
    people.select("name").show()

    println("Filter age over 21")
    people.filter(people("age") < 21).show()

    println("Group by Age")
    people.groupBy("age").count().show()

    println("Make 10 yrs older")
    people.select(people("name"),people("age")+10).show()

    spark.stop()
  }

}
