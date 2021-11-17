package com.assignment.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

/** Find the maximum exports for any given year */
object ExportInAnyYear {

  case class Exports(HSCode: Int, commodity: String, value: Double, country: String, year: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MaxExports")
      .master("local[*]")
      .getOrCreate()



    // Read the file as dataset
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/2018-2010_export.csv")
      .as[Exports]
    val maxExportsPerCommodityPerYear = ds.groupBy("year","commodity").agg(sum("value").as("SumVal")).sort("SumVal").select("commodity","year","SumVal")
    val maxExportsPerYear = maxExportsPerCommodityPerYear.select("*").groupBy("year").agg(max("SumVal"),last("commodity")).select("*")

    maxExportsPerYear.show(maxExportsPerYear.count().intValue())

    val results = maxExportsPerYear.collect()
    for (result <- results) {
      val year = result(0)
      val commodity = result(2)
      val sumVal = result(1)

      println(s"$commodity maximum exported in year: $year with total value $sumVal")
    }

  }
}