package com.study.spark.advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
import org.apache.log4j._

object MostObscureSuperHero extends App {
  case class HeroName(id: Int, name: String)
  case class HeroGraph(value: String)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("MostObscureSuperHerpDataset")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val HeroNameSchema = new StructType()
    .add("id", IntegerType, nullable = true)
    .add("name", StringType, nullable = true)

  val heroNameDataSet = spark.read
    .option("sep", " ")
    .schema(HeroNameSchema)
    .csv("data/Marvel-names.txt")
    .as[HeroName]
  val heroGraphDataSet = spark.read
    .text("data/Marvel-graph.txt")
    .as[HeroGraph]

  val heroRawConnections = heroGraphDataSet
    .withColumn("id", split($"value", " ")(0))
    .withColumn("connection", size(split($"value", " ")) - 1)

  val heroSumConnections = heroRawConnections
    .groupBy("id").agg(sum("connection").alias("connection"))

  val heroHasOneConnections = heroSumConnections
    .filter($"connection" === 1)
  val heroOneConnectionWithName = heroHasOneConnections
    .join(heroNameDataSet, heroHasOneConnections("id") === heroNameDataSet("id"))
    .drop(heroNameDataSet("id"))

  heroOneConnectionWithName.show(heroOneConnectionWithName.count.toInt, truncate = false  )

  val heroMinConnections =   heroSumConnections
    .agg(min("connection").alias("connection")).sort("connection")
  println(s"Min connection in the dataset is ${heroMinConnections.first()(0)}")

  spark.stop()
}
