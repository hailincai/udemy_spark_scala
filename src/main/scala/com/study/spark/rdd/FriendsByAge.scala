package com.study.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByAge extends App {

  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")

    return (fields(2).toInt, fields(3).toInt)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "FriendByAge")

  val lines = sc.textFile("data/fakefriends-noheader.csv")

  val rdd = lines.map(parseLine)

  /**
   * (33, 5)
   * (20, 4)
   * (33, 5)
   * ==> (33, (5+5, 1+1)) (20, (4, 1))
   */
  val totalFriends = rdd.mapValues((x) => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

  val avgFriends = totalFriends.mapValues((x) => x._1 / x._2)

  val result = avgFriends.collect()

  result.foreach(println)

  sc.stop

}
