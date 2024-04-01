package part2dataframes

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{approx_count_distinct, col, count, countDistinct}
import org.apache.spark.sql.functions._
object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))  // all the values except null
  genresCountDF.show()

  moviesDF.selectExpr("count(Major_Genre) as Total_Rows").show()  // all the values except null

  // Counting all
  moviesDF.select(count("*").as("Total_with_Nulls")).show()  // count all the rows, and will INCLUDE nulls

  // Counting distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // Approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  minRatingDF.show()

  moviesDF.selectExpr("min(IMDB_Rating)").show()

  // Avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")).as("Average_Rotten_Tomatoes_Rating")).show()
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

  // Data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))  // include nulls
    .count()  // select count(*) from moviesDF group by Major_Genre, SQL code equivalent
//  countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  avgRatingByGenreDF.show(25, truncate = false)

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count(col("*")).as("N_Movies"),
      avg(col("IMDB_Rating")).as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregationsByGenreDF.show()


  /*
  * Exercises
  *
  * 1. Sum up ALL the profits of ALL the movies in the DF
  * 2. Count how many distinct directors we have
  * 3. Show the mean and standard deviations of US gross revenue for the movies
  * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR (sort)
  */

//  1. Sum up ALL the profits of ALL the movies in the DF
  moviesDF.show(1, truncate = false)
  val profitsMoviesDF = moviesDF
    .select(
      (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Profits")
    )
    .select(sum("Total_Profits"))

  profitsMoviesDF.show(25, truncate = false)

//  2. Count how many distinct directors we have
  val distinctDirectorsDF = moviesDF.select(countDistinct(col("Director")))
  distinctDirectorsDF.show(50, truncate = false)

//  3. Show the mean and standard deviations of US gross revenue for the movies
  val statisticMoviesDF = moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  )

  statisticMoviesDF.show(10, truncate = false)

//  4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR (sort)
    val statisticsMoviesDF2  = moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      avg("US_Gross").as("Avg_US_Gross"),
      sum("US_Gross").as("Total_Gross")
    )
      .orderBy(col("Avg_IMDB_Rating").desc_nulls_last, col("Total_Gross").desc_nulls_last)

  statisticsMoviesDF2.show(10, truncate = false)
}
