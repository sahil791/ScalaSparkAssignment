package com.assignment.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Find the growth in trade between countries */
object TradeGrowthOFIndiaWithOtherCountries {

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
    *     3. How has the trade between India and any given country grown over time?
    * */
    val exports = dsExports.groupBy("year","country").agg(sum("value").as("ExportsValue")).sort("year")
    val imports = dsImports.groupBy("year","country").agg(sum("value").as("ImportsValue")).sort("year")
    val exportsImportsJoin = exports.join(imports, Seq("year","country")).sort("year").orderBy(asc("year"))
    val totalTrade = exportsImportsJoin.withColumn("totalTrade",$"ExportsValue" + $"ImportsValue")
    val windowSpec = Window.partitionBy("country").orderBy("year")
    import org.apache.spark.sql.functions._
    val selfDF = totalTrade.withColumn("percentageGrowth",
      (($"totalTrade" - when((lag("totalTrade", 1).over(windowSpec)).isNull, 0)
        .otherwise(lag("totalTrade", 1).over(windowSpec)))
        /(when((lag("totalTrade", 1).over(windowSpec)).isNull, 1).
        otherwise(lag("totalTrade", 1).over(windowSpec))))*100)


    selfDF.show(selfDF.count().intValue())
    //println(selfDF)
    val results = selfDF.collect()

    for (result <- results) {
      val year = result(0)
      val country = result(1)
      val percentage = result(5)
      println(s"Trade with $country in year $year is grown by $percentage %")
    }

  }
}