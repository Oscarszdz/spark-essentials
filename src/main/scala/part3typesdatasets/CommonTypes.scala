package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Adding a plain value to a DF (it works with whatever we want)
  moviesDF.select(col("Title"), lit(47).as("plain_value"))
//    .show(5, truncate = false)

  // Booleans
  // equivalents
//  moviesDF.select("Title").where(col("Major_Genre") === "Drama")
//  moviesDF.select("Title").where(col("Major_Genre") equalTo  "Drama")

  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

//  moviesDF.select("Title").where(dramaFilter)
  moviesDF.select("Title").where(preferredFilter)
//    .show(5, truncate = false)
  // + multiple ways of filtering

  // DF with Boolean values in Good_movie columns depending on preferredFilter
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("Good_movie"))

  // filter on a boolean columns
  moviesWithGoodnessFlagsDF
    .where("Good_movie")
//    .show(truncate = false)  // where(col("good_movie") === "true")

  // negations
  moviesWithGoodnessFlagsDF
    .where(not(col("Good_movie")))
//    .show(truncate = false)

  // Numbers

  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation Pearson = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")  /*corr is an ACTION*/)

  // strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

//  carsDF.show(5, truncate = false)

  // capitalization: initacap, lower, upper
  carsDF.select(initcap(col("Name")))
//    .show(20, truncate = false)

  // Contains (it is possible to chain multiple filters)
  carsDF.select("*").where(col("Name").contains("volkswagen"))
//    .show(5, truncate = false)

  // regex (this is the more powerful version of .contains)
  val regexString = "volkswagen|vw"

  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

//  vwDF
//    .show(5, truncate = false)

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )
//    .show(5, truncate = false)

  /*
  * Exercise
  *
  * Filter the cars DF by a list of car names obtained by an API call*/

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // version 1 - regex
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|")  // volkswagen|mercedes-benz|ford

  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")
//    .show(25, truncate = false)

  // version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)

//  carsDF.filter(bigFilter).show(25, truncate = false)


  }
