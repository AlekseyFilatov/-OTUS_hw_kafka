package kafka.zio.producer

import zio._
import zio.config.typesafe._
import zio.kafka.producer._
import zio.kafka.serde._
import zio.logging.backend._
import zio.stream._

object ProducerApp extends ZIOAppDefault {
  override val bootstrap: ZLayer[ZIOAppArgs, Nothing, Unit] =
    Runtime.setConfigProvider(ConfigProvider.fromResourcePath()) >>> Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override val run =
    (for {
      topic <- ZIO.config(AppConfigProducer.config.map(_.topic))
      _ <- ZStream
        .fromIteratorScoped(
          {
            val it = ZIO.fromAutoCloseable(
              ZIO.attempt(scala.io.Source.fromFile(s"${EventGenerator.workingDir}transactionRaw.csv"))
            ).map(_.getLines)
            it.map(x => x.map(x => EventGenerator.parser.parse(x)))
          }
        )
        .mapZIO { transaction =>
          (ZIO.logInfo("Producing transaction to Kafka...") *>
            Producer.produce(
              topic = topic,
              key = transaction.userId,
              value = transaction,
              keySerializer = Serde.long,
              valueSerializer = TransactionRaw.serde
            )) @@ ZIOAspect.annotated("userId", transaction.userId.toString)
        }
        .runDrain
    } yield ()).provide(
      producerSettingsProduce,
      Producer.live
    )

  private lazy val producerSettingsProduce =
    ZLayer {
      ZIO.config(AppConfigProducer.config.map(_.bootstrapServers)).map(ProducerSettings(_))
    }
}