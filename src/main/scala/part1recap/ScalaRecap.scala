package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expressions
  val anIfExpression = if(2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  // instructions or instructions like expression in Scala are denoted by the type unit.
  val theUnit: Unit = println("Hello, Scala")  // Unit = "no meaningful value" = void in other languages

  // functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal
  class Cat extends Animal
  trait Carnivore {
    // we can define abstracts = unimplemented methods
    def eat(animal: Animal): Unit
  }
  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch")
  }

  // singleton pattern (in one line I've defined both the type of my singleton and the single
  // instance that this type can have.)
  object MySingleton
  // companions (class or trait AND singleton with the same name in the same file)
  object Carnivore

  // generics
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2) // infix notation

  // Functional Programming (Functions in Scala are actually instances of a trait called Function1...Function22)
  val incrementer: Function1[Int, Int] = new Function[Int, Int] {
    override def apply(v1: Int): Int = x + 1
  }

  val incrementer_v2: Int => Int = new (Int => Int) {
    override def apply(v1: Int): Int = x + 1
  }

  val incrementer_v3: Int => Int = x => x + 1  // This is called anonymous function or lambda
  // when you define a lambda, you're actually instantiating one of those function and traits.
  val incremented = incrementer(42)

  // They are called Higher Order Function (meaning that they can receive functions as arguments)
  // map, flatMap, filter (These functions are in almost all collections in Scala)
  val processedList = List(1, 2, 3).map(incrementer)
  // We are going to use these HOF quite a lot when we process data sets and resilient distributed datasets/

  // Pattern Matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }
  println(ordinal)

  // try-catch
  try {
    throw new NullPointerException
  } catch {  // the catch is done via Pattern Matching
    case _: NullPointerException => "some returned value"
    case _ => "something else"
  }

  /*
  They are also a bunch of other use cases for pattern matching in real life code. For example, generators in
  for comprehensions (they are syntactic sugar for chains of map, flatMap and filter)
   */

  // Future (they abstract away the computations on separate threads)
  // implicit value used for as a platform for running thread
  import scala.concurrent.ExecutionContext.Implicits.global  // imported scope
  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  /*
  In order to process the value of the future, we can use the functional methods similar to the collection so we can say
  map, flapMat, filter on these features or .onComplete
   */

  aFuture.onComplete {
    // here I can run a pattern match on the possible values
    case Success(meaningOfLife) => println(s"I've found $meaningOfLife")
    case Failure(ex) => println(s"I've failed: $ex")
  }

  // Partial functions
  val aPartialFunction = (x: Int) => x match {
    case 1 => 43
    case 8 => 56
    case _ => 999  // anything else
  }

  val aPartialFunction_v2: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999  // anything else
  }

  // Implicits
  // auto-injection by the compiler
  def methodWithImplicitArgument(implicit  x: Int) = x + 43
  implicit val implicitInt = 67  // local scope
  val implicitCall = methodWithImplicitArgument
  println(implicitCall)

  // implicit conversions - implicit defs
  // case classes: lightweight data structures that have a bunch of utility methods already implemented by the compiler
  case class Person(name: String) {
    def greet = println(s"Hy, my name is $name ")
  }
  implicit def fromStringToPerson(name: String) = Person(name)
  "Mayte".greet  // fromStringToPerson("Mayte").greet

  // implicit conversion - implicit classes (Implicit classes are preferable to implicit defs)
  implicit class Dog(name: String) {
    def bark = println("Bark!")
  }

  "Lassie".bark

  /*
  The compiler figures out which implicit to inject here into the argument list is done by looking into:
    - local scope
    - imported scope
    - companion objects of the types involved in the method call
   */
  List(1, 2, 3).sorted



}
