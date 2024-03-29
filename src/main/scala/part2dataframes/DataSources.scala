package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession, types}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", FloatType),
      StructField("Cylinders", IntegerType),
      StructField("Displacement", FloatType),
      StructField("Hoserpower", IntegerType),
      StructField("Weight_in_ls", IntegerType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  /*
    Reading a DF:
      - format
      - schema or inferSchema = true
      - path
      - zero or more options
   */

  val carsDF = spark.read
    .format("json")
    .schema(carSchema)  // enforce a schema
    .option("mode", "failfast")  // dropMalformed, permissive (default)
    // Alternative way to load
//    .option("path", "src/main/resources/data/cars.json")
//    .load()
    .load("src/main/resources/data/cars.json")

  // alternative reading wit options map
  // Advantage of allowing us to compute our options dynamically at runtime
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
    Writing DFs
      - format
      - save mode = overwrite, append, ignore, errorIfExists
      - path
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
  // alternative
//    .option("path", "src/main/resources/data/cars_duplicated.json")
//    .save()
    .save("src/main/resources/data/cars_duplicated.json")

    // JSON flags
//    spark.read
//      .format("json")
//      .schema(carSchema)  // enforce a schema
////      .option("dateFormat", "YYYY-MM-dd")
//      // couple with schema; if Spark fails parsing, it will put null
//      .option("dateFormat", "yyyy-MM-dd")  // only works with force schema
////      .option("timeStamp", "string")
//      // Specifics to json
//      .option("allowSingleQuotes", "true")
//      .option("compression", "uncompressed")  // bzip, gzip, lz4 (7zip), snappy, deflate
//      .load("src/main/resources/data/cars.json")

  // alternative using:
//    .json("src/main/resources/data/cars.json") for this flag, you must delete .format("json") option

  // CSV flags (has the most possible flags here under the Spark DataFrame readers)
  val stocksSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  spark.read
//    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")
//    .load("src/main/resources/data/stocks.csv")

  // Parquet
carsDF.write
  .mode(SaveMode.Overwrite)
//  .parquet("src/main/resources/data/cars.parquet")  // this is pretty redundant
  // parquet is the default format in spark
  .save("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  spark.read.text("src/main/resources/data/sample_text.txt").show()

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /*
  * Exercise: read the movies DF, then write it as
  * - tab-separated values file
  * - snappy Parquet
  * - table "public.movies" in the Postgres DB
  * */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
//    .format("json")
//    .option("inferSchema", "True") // is already True
//    .option("mode", "failfast")  // maybe
//    .load("src/main/resources/data/movies.json")

  moviesDF.show()

//  Exercise #1: write DF as TSV
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .mode(SaveMode.Overwrite)
    .option("sep", "\t")
    .save("src/main/data/resources/data/moviesDF.csv")

//  Exercise #2: write DF as parquet
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/data/resources/data/moviesDF.parquet")
////    moviesDF.write.save("src/main/data/resources/data/moviesDF.parquet")

//  Exercise #3: Save to DB
//  moviesDF.write
//    .format("jdbc")
//    .option("driver", driver)
//    .option("url", url)
//    .option("user", user)
//    .option("password", password)
//    .option("dbtable", "public.movies")
//    .save()

  moviesDF.schema
}
