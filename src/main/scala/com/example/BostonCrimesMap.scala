package com.example

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.expressions.Window

object BostonCrimesMap extends App {

  val spark = SparkSession.builder().appName("Boston Crimes Map").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  val crimesPath = args(0)
  val offenseCodesPath = args(1)
  val outputPath = args(2)


  val crimesSchema = Encoders.product[Crime].schema
  val offenseCodeSchema = Encoders.product[OffenseCode].schema

  val crimes = spark.read.option("header", "true").schema(crimesSchema).csv(s"$crimesPath")
    .as[Crime].distinct
  val offenseCodes = spark.read.option("header", "true").schema(offenseCodeSchema).csv(s"$offenseCodesPath")
    .as[OffenseCode].distinct


  val fullDF = crimes.join(broadcast(offenseCodes), $"OFFENSE_CODE" === $"CODE")

  val basicAgg = crimes
    .groupBy("DISTRICT")
    .agg(
      count("*").as("crimes_total"),
      avg($"Lat").as("lat"),
      avg($"Long").as("lng")
    )

  val crimeFrequency = fullDF
    .withColumn("crime_type", split($"NAME", "-")(0))
    .groupBy($"DISTRICT", $"crime_type")
    .count()
    .orderBy(desc("count"))
    .withColumn("row", row_number.over(Window.partitionBy("DISTRICT").orderBy(desc("count"))))
    .where($"row" >=1 and $"row" <=3)
    .groupBy($"DISTRICT")
    .agg(collect_list($"crime_type").as("frequent_crime_types"))

  crimes
    .groupBy("DISTRICT", "YEAR", "MONTH")
    .count()
    .createOrReplaceTempView("monthly_grouped")

  val monthlyMedian = spark.sql(
    "SELECT DISTRICT, approx_percentile(count, 0.5) as crimes_monthly " +
      "FROM monthly_grouped " +
      "GROUP BY DISTRICT"
  ).toDF()

  val result = basicAgg
    .join(monthlyMedian, Seq("DISTRICT"))
    .join(crimeFrequency, Seq("DISTRICT"))
    .select(
      $"DISTRICT".as("district"),
      $"crimes_total",
      $"crimes_monthly",
      $"frequent_crime_types",
      $"lat",
      $"lng"
    )
    .as[DistrictAggregated]

  result
    .repartition(1)
    .write
    .mode("overwrite")
    .parquet(outputPath)
}

case class Crime(
                  INCIDENT_NUMBER: Option[String] = None,
                  OFFENSE_CODE: Option[Int] = None,
                  OFFENSE_CODE_GROUP: Option[String] = None,
                  OFFENSE_DESCRIPTION: Option[String] = None,
                  DISTRICT: Option[String] = None,
                  REPORTING_AREA: Option[Int] = None,
                  SHOOTING: Option[String] = None,
                  OCCURRED_ON_DATE: Option[Timestamp] = None,
                  YEAR: Option[Int] = None,
                  MONTH: Option[Int] = None,
                  DAY_OF_WEEK: Option[String] = None,
                  HOUR: Option[Int] = None,
                  UCR_PART: Option[String] = None,
                  STREET: Option[String] = None,
                  Lat: Option[Double] = None,
                  Long: Option[Double] = None,
                  Location: Option[String] = None
                )

case class OffenseCode(
                        Code: Option[Int] = None,
                        Name: Option[String] = None
                      )

case class DistrictAggregated(
                               district: String = null,
                               crimes_total: BigInt = 0,
                               crimes_monthly: BigInt = 0,
                               frequent_crime_types: String = null,
                               lat: Double = 0,
                               lng: Double = 0
                             )
