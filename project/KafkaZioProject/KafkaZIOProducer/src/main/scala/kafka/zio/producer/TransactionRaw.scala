package kafka.zio.producer

import kafka.zio.producer.protocol.deriveSerde
import zio.json._
import zio.kafka.serde._

final case class TransactionRaw(userId: Long, country: String, amount: BigDecimal)

object TransactionRaw {
  implicit val codec: JsonCodec[TransactionRaw] = DeriveJsonCodec.gen[TransactionRaw]

  lazy val serde: Serde[Any, TransactionRaw] = deriveSerde[TransactionRaw]
}