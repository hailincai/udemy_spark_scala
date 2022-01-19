package com.study.spark.advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
import org.apache.log4j._

object MostPopularSuperHeroDataset extends App {
  case class HeroName(id: Int, name: String)
  case class HeroGraph(value: String)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("MostPopularSuperHerpDataset")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val HeroNameSchema = new StructType()
    .add("id", IntegerType, nullable = true)
    .add("name", StringType, nullable = true)

  val heroNaeDataSet = spark.read
    .option("sep", " ")
    .schema(HeroNameSchema)
    .csv("data/Marvel-names.txt")
    .as[HeroName]
  val heroGraphDataSet = spark.read
    .text("data/Marvel-graph.txt")
    .as[HeroGraph]

  val heroConnections = heroGraphDataSet
    .withColumn("id", split($"value", " ")(0))
    .withColumn("connection", size(split($"value", " ")) - 1)
    .groupBy("id").agg(sum("connection").alias("connection"))
    .sort(desc("connection"))

  val mostPopularHeroId =
    heroConnections.first()

  val mostPopularHeroName = heroNaeDataSet
    .filter($"id" === mostPopularHeroId(0))
    .select("name")
    .first()


  println(s"Most popular hero name is: ${mostPopularHeroName(0)}")

  spark.stop()
}
