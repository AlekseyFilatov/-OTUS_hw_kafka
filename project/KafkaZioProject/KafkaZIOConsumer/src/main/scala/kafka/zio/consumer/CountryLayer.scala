package kafka.zio.consumer


import zio.stream.ZStream
import zio.{UIO, ULayer, URIO, URLayer, ZIO, ZLayer}

trait CountryLayer {
  def put(countryCache: CountryCache): URIO[CountryLayer, Unit]
}

object CountryLayer {
  def put(countryCache: CountryCache): URIO[CountryLayer, Unit] =
    ZIO.serviceWith[CountryLayer](_.put(countryCache))
}

final case class CountryLayerLive(countryCache: CountryCache) extends CountryLayer {
  override def put(countryCache: CountryCache): URIO[CountryLayer, Unit] =
  ZStream.fromIterable(CountryGenerator.countries)
      .mapZIO(
        country =>
          countryCache.put(country)).runDrain
}

object CountryLayerLive {
  lazy val layer: URLayer[CountryCache, CountryLayer] = ZLayer.fromFunction(CountryLayerLive(_))
}