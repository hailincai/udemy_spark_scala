package com.study.spark.sql

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountDataSet extends App {
  case class Book(value: String)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val se = SparkSession.builder()
    .appName("WordCountDataSet")
    .master("local[*]")
    .getOrCreate();

  import se.implicits._
  val fileContent = se.read
    .text("data/book.txt")
    .as[Book]

  // explode -- RDD's flatMap
  val words = fileContent.select(explode(split($"value", "\\W+")).alias("word"))
  val lowerWords = words.select(lower($"word").alias("word"))
  val wordCount = lowerWords.groupBy("word").count().orderBy(desc("count"))
  wordCount.show(5)

  fileContent.printSchema()

  se.stop()
}
