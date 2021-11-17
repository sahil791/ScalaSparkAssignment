package com.assignment.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.math.Ordered.orderingToOrdered

/** Find if India is net Exporter or net Importer for any given country in any year*/
object NetImporterOrExporterWithAnyCountry {

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
    * 4.  For any particular year and country find whether India was a net importer or exporter
    * */
    val exports = dsExports.groupBy("year","country").agg(sum("value").as("ExportsValue")).sort("ExportsValue")
    val imports = dsImports.groupBy("year","country").agg(sum("value").as("ImportsValue")).sort("ImportsValue")
    val t = exports.join(imports, Seq("year","country"))

    val results = t.collect()
      t.show()

    for (result <- results) {
      var e = 0.0
      var i = 0.0
      if(!result.isNullAt(2))
        e = result.getDouble(2)
      if(!result.isNullAt(3))
        i = result.getDouble(3)
      val year = result(0)
      val country = result(1)
      println(s"$e exports --  $i imports")

      if(e > i)
          println(s"India is net exporter in year: $year in trade with $country")
      else
        println(s"India is net importer in year: $year in trade with $country")
    }

  }
}