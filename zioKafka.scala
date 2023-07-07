package somepackage.org

import com.github.plokhotnyuk.jsoniter_scala.core.readFromString
import zio._
import zio.kafka.consumer.{CommittableRecord, Consumer, ConsumerSettings, OffsetBatch, Subscription}
import zio.kafka.serde.Serde
import zio.logging.LogFormat
import zio.logging.backend.SLF4J
import zio.stream.{ZSink, ZStream}

object KafkaToIcebergStreamer extends ZIOAppDefault {

  override val bootstrap = SLF4J.slf4j(LogLevel.Info, LogFormat.colored)

  private type KafkaRecordsString = CommittableRecord[String, String]

  private val subscription: Subscription = Subscription.topics("pixel-enriched")

  private val consumerWorkflow = Consumer
    .subscribeAnd(subscription)
    .plainStream(Serde.string, Serde.string)
    .aggregateAsyncWithin(ZSink.collectAllN[KafkaRecordsString](5000), Schedule.fixed(3.seconds))
    //.mapZIO { chunk => persistRecords(chunk.toList) *> OffsetBatch(chunk.map(_.offset)).commit }
    .mapZIO { chunk => persistRecords(chunk.toList)}


  private def transformKafkaRecordToMap(records: List[KafkaRecordsString]) = {
    val recordsMap = records.map(record => {
      val index = 999
      Map(
        "age" -> Integer.valueOf(index),
        "name" -> s"Name-$index",
        "city" -> s"City-$index"
      )
    })
    ZIO.succeed(recordsMap)
  }

  private def persistRecords(records: List[KafkaRecordsString]) =  {
    for {
      _ <- ZIO.logInfo(s"Writing ${records.size} records")
      icebergService <- ZIO.service[IcebergS3Writer]
      icebergRecords <- transformKafkaRecordToMap(records)
      _ <- icebergService.writeRecords(icebergRecords)
      _ <- ZIO.logInfo(s" ${records.size} written")
    } yield OffsetBatch(records.map(_.offset)).commit
  }


  private def consumerLayer =
    ZLayer.scoped(
      Consumer.make(
        ConsumerSettings(List("kafkacluster:9092"))
          .withGroupId("zioTestApp-01")
      )
    )

  private def icebergLayer =
      IcebergS3WriterImpl.getLive("test_iceberg_appender", "app_annie")

  override def run: ZIO[Any, Throwable, Unit] =
    consumerWorkflow
      .runDrain
      .provide(consumerLayer, icebergLayer)


}
