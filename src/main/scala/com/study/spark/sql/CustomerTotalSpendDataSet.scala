package com.study.spark.sql

import org.apache.log4j._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}

object CustomerTotalSpendDataSet extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class CustomerSpend(customerId: Int, itemId: Int, money: Float)

  val se = SparkSession.builder()
    .appName("CustomerTotalSpend")
    .master("local[*]")
    .getOrCreate()

  import se.implicits._
  val schema = new StructType()
    .add("customerId", IntegerType, nullable = false)
    .add("itemId", IntegerType, nullable = false)
    .add("money", FloatType, nullable = false)

  val rawData = se.read
    .schema(schema)
    .csv("data/customer-orders.csv")
    .as[CustomerSpend]

  val customerSpend = rawData
    .select("customerId", "money")
    .groupBy("customerId")
    .agg(round(sum("money"), 2).alias("spend"))
    .sort("spend")
    .select("customerId", "spend")

  val result = customerSpend.collect()

  for(row <- result){
    println(f"${row(0)} spend ${row(1).asInstanceOf[Double]}%.2f")
  }


  se.stop()
}
