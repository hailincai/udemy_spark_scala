package com.study.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RatingsCounter extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "RatingCount")

  val lines = sc.textFile("./data/ml-100k/u.data")

  val ratings = lines.map((line) => line.toString.split("\t")(2))

  val results = ratings.countByValue()

  val sortedResult = results.toSeq.sortBy(x => x._1)

  sortedResult.foreach(println)

  sc.stop
}
