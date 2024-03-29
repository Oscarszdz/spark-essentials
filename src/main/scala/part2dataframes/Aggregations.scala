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

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))

}
