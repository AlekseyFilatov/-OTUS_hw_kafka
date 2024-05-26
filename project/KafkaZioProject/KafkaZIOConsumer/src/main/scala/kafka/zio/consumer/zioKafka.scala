package kafka.zio.consumer

import zio._
import zio.http.Client
import zio.kafka.consumer._
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.{Serde, _}
import zio.logging.backend.SLF4J
import zio.stream.ZStream
import zio.config.typesafe.FromConfigSourceTypesafe
import zio.kafka.consumer.diagnostics.Diagnostics

object zioKafka extends ZIOAppDefault {

  override val bootstrap =
    Runtime.setConfigProvider(ConfigProvider.fromResourcePath()) >>> Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def run: ZIO[Any, Any, Unit] = (for {
    _ <- runLog("Starting operation","zio.logging.kafka")
    _ <- CountryCache.initCountryCache()
    appConfig <- ZIO.config(AppConfig.config)
    _ <- ZIO.logInfo(s"appConfig.consumer.topic: $appConfig.consumer.topic")
    _ <- Consumer
      .plainStream(
        subscription = Subscription.topics(appConfig.consumer.topic),
        keyDeserializer = Serde.long,
        valueDeserializer = TransactionRaw.serde
      ).mapZIO { committableRecord =>
        val offset = committableRecord.offset
        (for {
          transaction <- Enrichment.enrich(committableRecord.value)
          _ <- ZIO.logInfo("Producing enriched transaction to Kafka...")
          _ <- Producer.produce(
            topic = appConfig.producer.topic,
            key = transaction.userId,
            value = transaction,
            keySerializer = Serde.long,
            valueSerializer = TransactionEnriched.serde
          )
        } yield offset).catchAll { error =>
          ZIO.logError(s"Got error while processing: $error") *> ZIO.succeed(offset)
        } @@ ZIOAspect.annotated("userId", committableRecord.value.userId.toString)
      }.aggregateAsync(Consumer.offsetBatches)

      //.mapZIO(_.commit)
      .runDrain
      .catchAll(error => ZIO.logError(s"Got error while consuming: $error"))

    _ <- Consumer
      .plainStream(
        subscription = Subscription.topics(appConfig.consumer.topic),
        keyDeserializer = Serde.long,
        valueDeserializer = TransactionRaw.serde
      )
      .mapZIO { committableRecord =>
        val offset = committableRecord.offset
        (for {
          transaction <- Enrichment.enrich(committableRecord.value)
          _           <- ZIO.logInfo("Producing enriched transaction to Kafka...")
          _ <- Producer.produce(
            topic = appConfig.producer.topic,
            key = transaction.userId,
            value = transaction,
            keySerializer = Serde.long,
            valueSerializer = TransactionEnriched.serde
          )
        } yield offset).catchAll { error =>
          ZIO.logError(s"Got error while processing: $error") *> ZIO.succeed(offset)
        } @@ ZIOAspect.annotated("userId", committableRecord.value.userId.toString)
      }
      .aggregateAsync(Consumer.offsetBatches)
      .mapZIO(_.commit)
      .runDrain
      .catchAll(error => ZIO.logError(s"Got error while consuming: $error"))
  } yield ()).provide(
    EnrichmentLive.layer,
    CountryCacheLive.layer,
    Client.default,
    consumerSettings,
    Consumer.live,
    producerSettings,
    Producer.live,
    ZLayer.succeed(Diagnostics.NoOp),
    CountryLayerLive.layer)




  def runLog(info :String, head :String) :ZIO[Any, Any, Unit] =
    for {
      _ <- ZIO.logInfo(info) @@ zio.logging.loggerName(head)
      _ <- ZIO.logInfo("Confidential user operation") @@ SLF4J.logMarkerName("CONFIDENTIAL")
    } yield ()

  private lazy val producerSettings =
    ZLayer {
      ZIO.config(AppConfig.config.map(_.producer.bootstrapServers)).map(ProducerSettings(_))
    }

  private lazy val consumerSettings =
    ZLayer {
      ZIO.config(AppConfig.config.map(_.consumer)).map { consumer =>
        ConsumerSettings(consumer.bootstrapServers)
          .withGroupId(consumer.groupId)
          .withClientId("client")
          .withCloseTimeout(30.seconds)
          .withPollTimeout(10.millis)
          .withProperty("enable.auto.commit", "false")
          .withProperty("auto.offset.reset", "earliest")
      }
    }
}
