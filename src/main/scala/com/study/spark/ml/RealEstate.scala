package com.study.spark.ml

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.SparkSession
object RealEstate {
  case class RealEstate(HouseAge: Float, DistanceToMRT: Double, NumberConvenienceStores: Int, PriceOfUnitArea: Double);

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val se = SparkSession.builder()
      .appName("RealEstate")
      .master("local[*]")
      .getOrCreate()

    import se.implicits._
    val rawData = se.read
      .option("header", "true")
      .option("inferSchme", "true")
      .csv("data/realestate.csv")
      .as[RealEstate]

    val assembler = new VectorAssembler().
      setInputCols(Array("HouseAge, DistanceToMRT, NumberConvenienceStores"))
      .setOutputCol("features")
    val df = assembler.transform(rawData)
      .select("PriceOfUnitArea","features")

    // Let's split our data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    val dtr = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("PriceOfUnitArea")
    val model = dtr.fit(trainingDF)

    val fullPredictions = model.transform(testDF).cache()
    // Extract the predictions and the "known" correct labels.
    val predictionAndLabel = fullPredictions.select("prediction", "label").collect()
    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }

    se.stop()
  }
}
