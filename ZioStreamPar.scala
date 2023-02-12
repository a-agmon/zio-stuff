package org.aagmon.demos

import zio._
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Console, Queue, Schedule, ZIO, ZIOAppDefault}

object ZioStreamPar extends ZIOAppDefault{

  private def streamFromQueue(queue: Queue[Int]) =
    ZStream.fromQueueWithShutdown(queue)
      .mapZIO(i => ZIO.succeed(s"<$i>"))
      //.transduce(ZSink.collectAllN[String](20))
      .aggregateAsyncWithin(ZSink.collectAllN[String](20), Schedule.fixed(5.seconds))
      .run(streamSink)

  private def streamSink = ZSink.foreach((chunk: Chunk[String]) =>
    ZIO.debug(s">>>  we got ${chunk.length}")
     // *> ZIO.foreach(chunk)(ZIO.debug(_))
  )

  private def closeQueueIfEmpty(queue: Queue[Int]) =
    ZIO.ifZIO(queue.isEmpty)(
      onTrue = ZIO.debug(">>>  queue is empty, closing it") *> queue.shutdown,
      onFalse = ZIO.unit
    )

  private def runWorkflow(data:List[Int]) = for {
    taskQueue <- Queue.unbounded[Int]
    _ <- taskQueue.offerAll(data)
    _ <- closeQueueIfEmpty(taskQueue).repeat(Schedule.spaced(3.seconds)).fork
    _ <- ZIO.foreachPar(1 to 10)(_ => streamFromQueue(taskQueue))
    _ <- ZIO.debug(">>>  done")
  } yield ()

  val data:List[Int] = List.range(1, 100)
  override def run: ZIO[Any, Any, Any] = runWorkflow(data)

}
