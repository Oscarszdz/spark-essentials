package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
//  val moviesWithReleasesDates = moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-Mmm-yy").as("Actual_Release"))  // conversion
//
//    moviesWithReleasesDates
//    .withColumn("Today", current_date())  // today
//    .withColumn("Right_Now", current_timestamp())  // this second
//    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)  // date_add, date_sub

//  moviesWithReleasesDates.select("*").where(col("Actual_Release").isNull).show(25, truncate = false)

  /*
  * Exercise
  * 1. How do we deal with multiple date formats?
  * 2. Read the stocks DF and parse the dates
  */

  // 1. How do we deal with multiple date formats?
  // a) parse the DF multiples times, then union the small DFs


  // 2. Read the stocks DF and parse the dates
    val stocksDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/data/stocks.csv")

//  stocksDF.show(5, truncate = false)
//  stocksDF.printSchema()

//  val stocksDFWithDates = stocksDF
//    .withColumn("actual_date", to_date(col("date"), "m dd yyyy"))

//  stocksDFWithDates.show(5, truncate = false)

  // Structures

  // 1 - with column operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
//    .show(5, truncate = false)

  // 2 - with expressions strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
//    .selectExpr("Title", "Profit.US_Gross")
//    .selectExpr("Title", "Profit.US_Gross as Gross")
//    .show(3, truncate = false)

  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
        .selectExpr("Title", "Profit.US_Gross")
    //    .selectExpr("Title", "Profit.US_Gross as Gross")
//    .show(3, truncate = false)

  // Arrays
  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_words"))  // ARRAY of strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  )
//    .show(25, truncate = false)


}
