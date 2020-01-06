package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BostonCrimesMap extends App {

  val spark = SparkSession.builder().appName("Boston Crimes Map").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  val crimesPath = args(0)
  val offenseCodesPath = args(1)
  val outputPath = args(2)

  val crimes = spark.read.option("header", "true")
    .csv(s"$crimesPath")

  val offenseCodes = spark.read.option("header", "true")
    .csv(s"$offenseCodesPath")


  val result = crimes.join(offenseCodes, $"OFFENSE_CODE" === $"CODE")
      .groupBy($"DISTRICT")
      .agg(
        count($"INCIDENT_NUMBER").as("crimes_total"),
        $"".as("crimes_monthly"),
        $"".as("frequent_crime_types"),
        $"".as("lat"),
        $"".as("lng")
      )

  result
    .repartition(1)
    .write
    .mode("overwrite")
    .parquet(outputPath)
}
