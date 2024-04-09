package part3typesdatasets

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.sql.Date

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()


  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS
    .filter(_ < 50)
//    .show()

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Double,
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Long,
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )



  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")


  // 3 - define an encoder (importing the implicits)
  import spark.implicits._
  val carsDF = readDF("cars.json")

  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS
    .filter(_ < 100)
//    .show()

// map, flatMap, fold, reduce, filter, for comprehensions ...
  val carNameDS = carsDS.map(car => car.Name.toUpperCase())

  carNameDS
    .show(5, truncate = false)

}
