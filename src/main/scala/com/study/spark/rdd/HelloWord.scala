package com.study.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object HelloWord extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "HelloWorld")

  val lines = sc.textFile("data/ml-100k/u.data")
  val numLines = lines.count()

  println("Hello world! The u.data file has " + numLines + " lines.")

  sc.stop()

}
