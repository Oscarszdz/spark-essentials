package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFramesBasics extends App {

   // In order to actually run an application on top of Spark, we need a Spark session

  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // reading a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // showing a DF
  firstDF.show()
  firstDF.printSchema()

  // get rows
  firstDF.take(10).foreach(println)

  // spark types
  val longType = LongType

  // schema
  val carSchema = StructType(
    Array(
      StructField("Name", StringType), // nullable = true -- by default
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Anio", StringType),
      StructField("Origin", StringType)
    )
  )

  // obtain a schema
  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
//    .schema(carSchema)
    .load("src/main/resources/data/cars.json")

  carsDFWithSchema.show()

  // CODE BELOW IS VERY USEFUL FOR TESTING PURPOSES
  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA")

  // create DF from tuples
  val cars = Seq(
  ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
  ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
  ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
  ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
  ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
  ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
  ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
  ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
  ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
  ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  val manualCarsDF = spark.createDataFrame(cars)  // schema auto-inferred

  // note: DFs have schemas, rows do note

  // create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF(
    "Aceleración", "Cilindros", "Kilometraje", "H.P.", "Millas_por_galón", "Nombre", "Origen", "Peso_en_Libras", "Año"
  )

//  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  manualCarsDFWithImplicits.show()



  /*
    * Exercise:
    * 1) Create a manual DF describing smartphones
    * - make
    * - model
    * - screen dimension
    * - camera megapixels
    *
    * 2) Read another file from the /data folder (movies.json)
    *   - print its schema
    *   - count the number of rows, call count()
   */

  // 1) Create a manual DF describing smartphones
  val smarthphoneSchema = StructType(
    Array(
      StructField("make", StringType),
      StructField("model", FloatType),
      StructField("screen_dimension", StringType),
      StructField("camera", IntegerType)
    )
  )

  val smartphone = Seq(
    ("iPhone", "15_Pro_Max", 6.7, 48),
    ("Samsung", "Galaxy_S23_Ultra", 6.8, 200),
    ("Google", "Pixel_8_Pro", 6.7, 50),
    ("Samsung", "Galaxy_Z_Flip_5", 6.7, 20),
    ("One_Plus", "11_5G", 6.7, 50)
  )


//  val smartphoneDF = spark.createDataFrame(smartphone)
  val smartphoneDF = spark.createDataFrame(smarthphoneSchema)
  smartphoneDF.printSchema()
  smartphoneDF.show()


}
