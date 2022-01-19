package com.study.spark.advanced

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object PopularMovieDataSet extends App {
  final case class Movie(movieId: Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("PopularMovieDataSet")
    .master("local[*]")
    .getOrCreate()

  val movieSchema = new StructType()
    .add("userId", IntegerType, nullable = true)
    .add("movieId", IntegerType, nullable = true)
    .add("rating", IntegerType, nullable = true)
    .add("timestamp", LongType, nullable = true)

  import spark.implicits._
  val movieData = spark.read
    .option("sep", "\t")
    .schema(movieSchema)
    .csv("data/ml-100k/u.data")
    .as[Movie]

  val sortedMovieData = movieData.groupBy("movieId").count().sort(desc("count"))
  sortedMovieData.show(10)

  spark.stop()
}
