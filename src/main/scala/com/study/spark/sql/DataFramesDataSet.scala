package com.study.spark.sql

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object DataFramesDataSet extends App {
  case class Person(id: Int, name: String, age: Int, friends: Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val se = SparkSession.builder()
    .appName("SparkSQLDataSet")
    .master("local[*]")
    .getOrCreate()

  import se.implicits._
  val personSchema = se.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/fakefriends.csv")
    .as[Person]

  personSchema.printSchema()

  println("select name only")
  // Simple one can use column name directly
  personSchema.select("name", "age").show(2)
  // if need to apply action to colum, using select(Column*)
  personSchema.select(personSchema("name"), (personSchema("age") + 10).alias("new_age")).show(2)

  println("filter age < 19")
  personSchema.filter("age <= 19").select("name", "age").show(2)

  println("group by")
  personSchema.groupBy("age").mean("friends").withColumnRenamed("avg(friends)", "avg_friends").sort("age").show(10)
  personSchema.groupBy("age").agg(functions.round(avg("friends"), 2).alias("avg_friend")).sort("age").show(2)


  se.stop()
}
