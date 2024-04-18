package part3typesdatasets

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date
import scala.math.Ordered.orderingToOrdered

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
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
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
  //    .show(5, truncate = false)

  /*
  * Exercises
  *
  * 1. Count many cars we have
  * 2. Count how many POWERFUL cars we have (HP > 140)
  * 3. Average HP for the entire dataset
  * */

  // 1. Count many cars we have
  val carsCount = carsDS.count()
  println(carsCount)

  //  2. Count how many POWERFUL cars we have (HP > 140)
    println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)
  //  3. Average HP for the entire dataset
    println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // also use the DF functions!
  carsDS.select(avg(col("Horsepower"))).show()

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
  guitarPlayerBandsDS
//    .show(5, truncate = false)

  /*Note
  * Cannot up cast `id` from bigint to int as it may truncate
  * When we do .option("inferSchema", "true") Spark will read all the numbers as longs (big int = Long)*/

   /*
   * Exercise: join the guitarDS and guitarPlayersDS, in an outer join
   * (hint: use array_contains)
  */

  val guitarsPlayersDS: Dataset[(GuitarPlayer, Guitar)] = guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")

  guitarsPlayersDS
//    .show(5, truncate = false)

  // GroupingCars

  val carsGroupByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations
  // Be careful when using joins and groups for performance

}