package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

//  guitarsDF.show(25, truncate = false)
//  guitaristDF.show(25, truncate = false)
//  bandsDF.show(25, truncate = false)

  // inner joins
  val joinCondition = guitaristDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF.show(25, truncate = false)

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls where the data is missing
//  guitaristDF.join(bandsDF, joinCondition, "left_outer").show()

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls where the data is missing
//  guitaristDF.join(bandsDF, joinCondition, "right_outer").show()

  // outer join = everything in the inner join + all the rows in the BOTH table, with nulls where the data is missing
//  guitaristDF.join(bandsDF, joinCondition, "outer").show()

  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
//  guitaristDF.join(bandsDF, joinCondition, "left_semi").show()

  // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
//  guitaristDF.join(bandsDF, joinCondition, "left_anti").show()

  // thing to bear in mind
//  guitaristsBandsDF.select("id", "band").show()  // this crashes

  // how to solve it
  // option 1 - rename the column on which we are joining
  guitaristsBandsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
//  val bandsModifiedDF = bandsDF.withColumnRenamed("id", "bandId")
//  guitaristDF.join(bandsModifiedDF, guitaristDF.col("band") === bandsModifiedDF.col("bandId"))

  // using complex types
  val newDF = guitaristDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarID)"))
  newDF.show()

  /* Exercises
  *
  * - show all employees and their max salary
  * - show all employees who were never managers
  * - find the job titles of the best paid 10 employees in the company*/

//  show all employees and their max salary
  // First read the tables from the db
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

  val salariesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.salaries")
    .load()

//  employeesDF.show(5, truncate = false)
//  salariesDF.show(5, truncate = false)

  val maxSalaries = salariesDF
    .groupBy("emp_no")
    .agg(max("salary"))
    .orderBy(col("max(salary)").desc)

  maxSalaries.show(5, truncate = false)

//  show all employees who were never managers
  val deptManagerDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.dept_manager")
    .load()

  val joinConditionDF = employeesDF.col("emp_no") === deptManagerDF.col("emp_no")
  val employeesNoManagerDF = employeesDF.join(deptManagerDF, joinConditionDF, "left_anti")

  employeesNoManagerDF.show(10, truncate = false)

//  find the job titles of the best paid 10 employees in the company
  val titlesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.titles")
    .load()

  val joinConditionBestPaid = maxSalaries.col("emp_no") === titlesDF.col("emp_no")
  val bestPaidEmployeesDF = maxSalaries.join(titlesDF, joinConditionBestPaid)

  bestPaidEmployeesDF.show(10, truncate = false)

  // Solved with the master
  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF1 = readTable("employees")
  val salariesDF1 = readTable("salaries")
  val deptManagersDF1 = readTable("dept_manager")
  val titlesDF1 = readTable("titles")

  // 1
  val maxSalariesPerEmpNoDF = salariesDF1.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF1.join(maxSalariesPerEmpNoDF, "emp_no")

  employeesSalariesDF.show(5, truncate = false)

  // 2
  val empNeverManagersDF = employeesDF1.join(
    deptManagersDF1, employeesDF1.col("emp_no") === deptManagersDF1.col("emp_no"),
  "left_anti")
  empNeverManagersDF.show()

  // 3
  val mostRecentJobTitlesDF = titlesDF1.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF1 = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF1.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show(10, truncate = false)

}
