package com.appsflyer.bdnd

import zio._
import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{Console, Queue, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}

//noinspection SimplifyForeachInspection
object ZioGitlabAPI extends ZIOAppDefault{

  private def insertJobs(jobs:List[Job]) = ZIO.attempt{
    jobs.foreach(job => println(s"\tprocessing job:${job.id}"))
  }
  private def processStream(queue: Queue[PipelineData]) =
    ZStream.fromQueueWithShutdown(queue)
      .mapZIOPar(50)(p => ZIO.attempt(JobsHandler.paginationJobsNoFuture(p.projectId, p.pipelineId)))
      .mapZIOPar(50)(insertJobs)

  private def doSomethingWithJobs(jobs: List[Job]): Unit = {}

  private case class PipelineData(projectId:String, pipelineId:String)

  private val workflow = for{
    _ <- ZIO.logInfo("starting projects")
    projects <- ZIO.attempt(ProjectsHandler.paginationProject())
    _ <- ZIO.logInfo("starting project pipelines")
    projectPipelines <- ZIO.foreachPar(projects)(proj => ZIO.attempt(PipelinesHandler.paginationPipelinesNonFuture(proj.id)))
    _ <- ZIO.logInfo(s"starting pipelines of projects: ${projects.length}}")
    pipelines <- ZIO.attempt(projectPipelines.flatMap(project => project.pipelines.map(pipeline => PipelineData(project.projectId, pipeline.id))))
    _ <- ZIO.logInfo(s"received ${pipelines.length} pipelines")
    pipelineQueue <- Queue.unbounded[PipelineData]
    x <- pipelineQueue.offerAll(pipelines)
    _ <- ZIO.logInfo(s"added ${x} pipelines to queue (${pipelineQueue})")
    _ <- processStream(pipelineQueue).runDrain

//    _ <- ZIO.foreachParDiscard( 0 to 50)(i => {
//      pipelineQueue.poll.flatMap{
//        case Some(p) => ZIO.attempt(JobsHandler.paginationJobsNoFuture(p.projectId, p.pipelineId))
//        case None => pipelineQueue.shutdown
//      }
//    }.forever)
    _ <- ZIO.logInfo(s"done with jobs")
  } yield ()



  val data: Seq[Int] =  List.range(1, 1000, 1)
  val streamFlow: ZStream[Any, Throwable, Unit] = ZStream.fromIterable(data)
    .mapZIOPar(1)(a => Console.printLine(s"$a") *> ZIO.attempt(a))
    .mapZIOPar(1)(b => ZIO.attempt(s"Number $b"))
    .mapZIOPar(1)(c => Console.printLine(c))


  val pagingTest = ZStream.fromZIO(Random.nextInt)

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] = streamFlow.runDrain
}
