package in.iitpkd.scala
import org.apache.spark._
import org.apache.log4j._
import scala.math.min

object MinTemparatures {

  def parseLine(line:String) = {

    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f

    (stationId,entryType,temperature)
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","MinTemparatures")

    val lines = sc.textFile("data/weather.csv")

    val parsedLines = lines.map(parseLine)

    // Checking for TMIN attribute
    val minTemps = parsedLines.filter( x=> x._2 == "TMIN")

    val stationTemps = minTemps.map( x => (x._1, x._3.toFloat))

    // takes two values x,y and returns min(x,y)
    val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y))

    val results = minTempsByStation.collect()

    for(result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f C"
      println(s"$station minimum temperature : $formattedTemp")
    }
  }
}
