package com.study.spark.sql

import org.apache.log4j._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._

object MinTemperatureDataSet extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class StationTemperature(stationId: String, time: Int, measureType: String, temperature: Float)

  val se = SparkSession.builder()
    .appName("MinTemperatureDataSet")
    .master("local[*]")
    .getOrCreate()

  import se.implicits._
  val schema = new StructType()
    .add("stationId", StringType, nullable = true)
    .add("time", IntegerType, nullable = true)
    .add("measureType", StringType, nullable = true)
    .add("temperature", FloatType, nullable = true)

  val rawData = se.read
    .schema(schema)
    .csv("data/1800.csv")
    .as[StationTemperature]

  val stationMinTemperatures = rawData.filter($"measureType" === "TMIN")
  val stationMinTemperature = stationMinTemperatures.groupBy("stationId")
    .agg(min("temperature").alias("temperature"))
    .withColumn("temperature", round($"temperature" * 0.1f * (9.0f / 5.0f) + 32.0f, 2) )
    .select("stationId", "temperature")

  stationMinTemperature.show(stationMinTemperature.count.toInt)

  val result: Array[Row] = stationMinTemperature.collect()

  for (row <- result){
    println(f"${row(0)} min temperature is: ${row(1).asInstanceOf[Float]}%.2f")
  }

  se.stop()
}
