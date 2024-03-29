package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, column, desc, exp, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // How to create Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

//  carNamesDF.show(20, truncate = false)

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

//  carsWithWeightsDF.show(20, truncate = false)

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

//  carsWithSelectExprWeightsDF.show(20, truncate = false)

  // DF processing

  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")  // which does the exact same thing
  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower  > 150" )

  // unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", value = true)
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF)  // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show(25, truncate = false)

  allCountriesDF
    .sort(asc("Origin"))
    .show(25, truncate = false)

  allCarsDF.show(1000, truncate = false)

  /*
  * 1. Read the movies DF and select 2 columns of your choice.
  * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD_Sales
  * 3. Select all COMEDY movies with IMDB rating above 6.
  *
  * Use as many versions as possible
  */

// 1. Read the movies DF and select 2 columns of your choice.
  val moviesOsdDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesOsdDF.show(2, truncate = false)
// option a)
  moviesOsdDF.select("Title", "Major_Genre").show(2, truncate = false)
// option b)
  moviesOsdDF.select(col("Director"), col("Distributor")).show(2, truncate = false)
//  option c)
  moviesOsdDF.select(column("IMDB_Rating"), column("IMDB_Votes")).show(2, truncate = false)
//  option d)
  moviesOsdDF.select('Running_Time_min, 'Source).show(10, truncate = false)  // deprecated in Scala 2.13
//  option e)
  moviesOsdDF.select($"Major_Genre", $"Production_Budget").show(2, truncate = false)
//  option f)
  moviesOsdDF.select(expr("Title"), expr("Release_Date")).show(2, truncate = false)
//  option e)
  moviesOsdDF.selectExpr("Title", "Major_Genre").show(2, truncate = false)

//2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD_Sales
//  option a)
  val sumProfitColumn = moviesOsdDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )
  sumProfitColumn.show(10, truncate = false)

//  option b)
  val sumProfitColumn2 = moviesOsdDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )
  sumProfitColumn2.show(3, truncate = false)

//  option c)
  val sumProfitColumn3 = moviesOsdDF.select(
    "Title",
    "US_Gross",
    "Worldwide_Gross"
  ).withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

  sumProfitColumn3.show(1, truncate = false)

////  3. Select all COMEDY movies with IMDB rating above 6.
//  option a)
  moviesOsdDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).show(2, truncate = false)
//  option a.2)
//  val Genre = "Comedy"
//  val Rating = 6
//  moviesOsdDF.filter(col("Major_Genre") === Genre and col("IMDB_Rating") > Rating).show(10, truncate = false)

  //  option b)
  moviesOsdDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6").show(5, truncate = false)

//  option c)
  moviesOsdDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).show(2, truncate = false)

  // option d)
  moviesOsdDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)
    .sort(desc("IMDB_Rating"))
    .show(25, truncate = false)
}
