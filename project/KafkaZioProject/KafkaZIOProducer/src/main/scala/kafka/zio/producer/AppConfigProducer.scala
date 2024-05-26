package kafka.zio.producer

import zio.config.magnolia.deriveConfig

final case class AppConfigProducer(bootstrapServers: List[String], topic: String)

object AppConfigProducer {
  lazy val config = deriveConfig[AppConfigProducer]
}
