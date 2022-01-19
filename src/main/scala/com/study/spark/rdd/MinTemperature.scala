package com.study.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MinTemperature extends App {
  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "MinTemperature")

  val lines = sc.textFile("data/1800.csv")

  val rawData = lines.map(parseLine)

  val stationTemperatures = rawData.filter((data) => data._2 == "TMIN").map((data) => (data._1, data._3.toFloat))

  val result = stationTemperatures.reduceByKey((x, y) => Math.min(x, y)).collect()

  for (row <- result.sorted) {
    println(f"Station[${row._1}]'s min temperature is ${row._2}%.2f")
  }

  sc.stop
}
