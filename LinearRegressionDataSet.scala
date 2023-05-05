package in.iitpkd.scala
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._

object LinearRegressionDataSet {

  case class RegressionSchema(label: Double, features_raw: Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .getOrCreate()

    // label are first col, features are second col
    val regressionSchema = new StructType()
      .add("label", DoubleType, nullable = true)
      .add("features_raw", DoubleType, nullable = true)

    import spark.implicits._
    val dsRaw = spark.read
      .option("sep",",")
      .schema(regressionSchema)
      .csv("data/regression.txt")
      .as[RegressionSchema]

    val assembler = new VectorAssembler().
      setInputCols(Array("features_raw")).
      setOutputCol("features")
    val df = assembler.transform(dsRaw)
      .select("label","features")

    //splitting data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5,0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    // Now create linear regression mode
    val lir = new LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(100)
      .setTol(1E-6)

    // Train
    val model = lir.fit(trainingDF)

    // See for preds, generate pred
    val fullPredictions = model.transform(testDF).cache()

    // Extract predictions
    val predictionAndLabel = fullPredictions.select("prediction","label").collect()

    // Print
    for(prediction <- predictionAndLabel) {
      println(prediction)
    }

    spark.stop()

  }
}
