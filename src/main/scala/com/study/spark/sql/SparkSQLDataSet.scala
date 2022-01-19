package com.study.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object SparkSQLDataSet extends App {
  case class Person(id: Int, name: String, age: Int, friends: Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val se = SparkSession.builder()
    .appName("SparkSQLDataSet")
    .master("local[*]")
    .getOrCreate()

  import se.implicits._
  val schemaPerson = se.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/fakefriends.csv")
    .as[Person]

  schemaPerson.printSchema()

  schemaPerson.createOrReplaceTempView("people")

  val teenages = se.sql("select * from people where age >= 13 and age <= 19")

  val results = teenages.collect()

  val avgFriends = se.sql("select age, avg(friends) as avg_friend from people group by age order by age");

  val avgFriendsResult = avgFriends.collect();

  avgFriendsResult.foreach(println)

  se.stop()
}
