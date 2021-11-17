package com.assignment.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Find if India is net Exporter or net Importer in any given year*/
object NetImporterOrExporterForAnyYear {

  case class Exports(HSCode: Int, commodity: String, value: Double, country: String, year: Int)
  case class Imports(HSCode: Int, commodity: String, value: Double, country: String, year: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("NetImporterOrExporter")
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
    * 5. For a year, find if India was a net importer or exporter
    * */
    val exports = dsExports.groupBy("year").agg(sum("value").as("ExportsValue")).sort("ExportsValue")
    val imports = dsImports.groupBy("year").agg(sum("value").as("ImportsValue")).sort("ImportsValue")
    val t = exports.join(imports, "year")

    val results = t.collect()
      t.show()

    for (result <- results) {
      var e = 0.0
      var i = 0.0
      if(!result.isNullAt(1))
        e = result.getDouble(1)
      if(!result.isNullAt(2))
        i = result.getDouble(2)
      val year = result(0)
      println(s"$e exports --  $i imports")

      if(e > i)
          println(s"India is net exporter in year: $year ")
      else
        println(s"India is net importer in year: $year ")
    }

  }
}