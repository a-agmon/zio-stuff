package org.aagmon.demos

import zio.{ZIO, ZIOAppDefault}
import zio._

case class ServiceEndpointError(msg:String) extends Exception(msg)

object ZioDemo extends ZIOAppDefault {

  private def legacyCallMethodB(someRandomString: String) : Boolean = {
    println("Running legacy function with => " + someRandomString)
    val someNum = rand.between(0, 10)
    println("Running function with => " + someNum)
    someNum match {
      case happyPath if 0 to 2 contains happyPath =>
        println("We are in the happy path")
        true
      case lessHappyPath if 3 to 7 contains lessHappyPath =>
        println("We are in the LESS happy path")
        false
      case sadPath if 8 to 10 contains sadPath =>
        println("We are in the SAD path  - blowing up")
        val boom = 5/0
        true
    }
  }

  private def callMethodB(str:String) = {
    val response = ZIO.attempt(legacyCallMethodB(str))
    ZIO.ifZIO(response)(
      onTrue = ZIO.succeed(true),
      onFalse = ZIO.fail(ServiceEndpointError("we didnt get an answer on time"))
    )
  }


  private def callMethodA(someString:String): Unit = println(s"got string: $someString")
  private val rand = new scala.util.Random

  private val mainWorkflow = for {
    _ <- ZIO.debug("starting app")
    _ <- callMethodB("some random string")
      .retry(
        Schedule.recurs(5) && Schedule.spaced(3.seconds)
      ).orDie
    _ <- ZIO.debug("done")

  } yield ()


  private val workflowSimple = for {
    _ <- ZIO.logInfo("starting app")
    result <- ZIO.attempt(legacyCallMethodB("some string"))
      .catchAll(e => {
        ZIO.logInfo(s"We got an error: ${e.getMessage}  continuing with false value")
        *> ZIO.succeed(false)
      })
      .repeat(
        Schedule.recurs(2) && Schedule.spaced(3.seconds) && Schedule.recurUntil(r => r)
      )
    _ <- ZIO.logInfo(s"final result is $result")
  } yield ()

  override def run: ZIO[Any, Throwable, Unit] = workflowSimple

}





