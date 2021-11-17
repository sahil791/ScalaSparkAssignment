package com.assignment.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Find the maximum exports or imports for any given commodity */
object MajorCommodityInTrade {

  case class Exports(HSCode: Int, commodity: String, value: Double, country: String, year: Int)
  case class Imports(HSCode: Int, commodity: String, value: Double, country: String, year: Int)

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
    val dsExports = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/2018-2010_export.csv")
      .as[Exports]
    val dsImports= spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/2018-2010_import.csv")
      .as[Imports]
    /*
    *     2. Which commodity forms a major chunk of trade? Does it conform to theories of international trade?
    * */
    val maxExportsPerCommodity = dsExports.groupBy("commodity").agg(sum("value").as("SumVal")).sort("SumVal").select("commodity","SumVal")
    val maxExports = maxExportsPerCommodity.select("*").agg(max("SumVal"),last("commodity")).select("*")

    val maxImportsPerCommodity = dsImports.groupBy("commodity").agg(sum("value").as("SumVal")).sort("SumVal").select("commodity","SumVal")
    val maxImports = maxImportsPerCommodity.select("*").agg(max("SumVal"),last("commodity") as("commodity")).select("*")

    /**
     *  Maximum exported or imported commodity is
     *  MINERAL FUELS, MINERAL OILS AND PRODUCTS OF THEIR DISTILLATION; BITUMINOUS SUBSTANCES; MINERAL WAXES.
     *  And We got the answer as expected, which conforms the international trade theory.
     */
    maxExports.show()
    maxImports.show()

    val resultsExp = maxExports.collect()
    val resultImp = maxImports.collect()

    for (result <- resultsExp) {
      val commodity = result(1)
      println(s"$commodity maximum exported ")
    }

    for (result <- resultImp) {
      val commodity = result(1)
      println(s"$commodity maximum imported ")
    }

  }
}