package com.study.spark.advanced

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}
import scala.util.Try

object PopularMovieWithNameUsingBroadcast extends App {
  final case class Movie(movieId: Int)

  def loadMovieName: Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8.

    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item")

    for (line <- lines.getLines()){
      // when using "" for split, it is a regular expression
      // when using '' for split, it is a regular string split
      val fields = line.split("\\|")
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    lines.close()

    movieNames
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("PopularMovieDataSet")
    .master("local[*]")
    .getOrCreate()

  val nameBroadcast = spark.sparkContext.broadcast(loadMovieName)

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

  val movieCount = movieData.groupBy("movieId").count()

  val lookupMovieName: Int => String = (movieId: Int) => {
    nameBroadcast.value(movieId)
  }

  val lookupMovieNameUDF = udf(lookupMovieName)

  val movieDataWithName = movieCount
    .withColumn("title", lookupMovieNameUDF($"movieId"))
    .sort(desc("count"))

  movieDataWithName.show()

  spark.stop()
}
