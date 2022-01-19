package com.study.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerTotalSpend extends App {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val spend = fields(2).toFloat

    (customerId, spend)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "CustomerTotalSpend")

  val lines = sc.textFile("data/customer-orders.csv")
  val customerInfo = lines.map(parseLine)

  val customerTotalSpend = customerInfo.reduceByKey((x, y) => x + y)

  val sortedCustomerTotalSpend = customerTotalSpend.map((row) => (row._2, row._1)).sortByKey().collect()

  for (row <- sortedCustomerTotalSpend) {
    val customerId = row._2
    val totalSpend = row._1

    println(f"${customerId} : ${totalSpend}%.2f")
  }
}
