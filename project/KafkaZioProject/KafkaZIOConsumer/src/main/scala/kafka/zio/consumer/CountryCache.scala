package kafka.zio.consumer


import zio._
import zio.macros._
import zio.stream.ZStream

//@accessible
trait CountryCache {
  def get(countryName: String): UIO[Option[Country]]

  def put(country: Country): UIO[Unit]

  def initCountryCache(): UIO[Unit]
}

object CountryCache {
  /*def get(countryName: String): ZIO[CountryCache, Any, Option[Country]] =
    ZIO.serviceWithZIO[CountryCache](_.get(countryName))

  def put(country: Country): ZIO[CountryCache, Any, Unit] =
    ZIO.serviceWithZIO[CountryCache](_.put(country))*/
  def get(countryName: String): URIO[CountryCache, Option[Country]] =
  ZIO.serviceWithZIO[CountryCache](_.get(countryName))

  def put(country: Country): URIO[CountryCache, Unit] =
  ZIO.serviceWithZIO[CountryCache](_.put(country))

  def initCountryCache(): URIO[CountryCache, Unit] =
    ZIO.serviceWithZIO[CountryCache](_.initCountryCache())
}

final case class CountryCacheLive(ref: Ref[Map[String, Country]]) extends CountryCache {
  self =>
  override def get(countryName: String): UIO[Option[Country]] =
    (for {
      _ <- ZIO.logInfo(s"Getting country details from cache.")
      cache <- self.ref.get
      result <- ZIO.succeed(cache.get(countryName))
    } yield result) @@ ZIOAspect.annotated("countryName", countryName)

  override def put(country: Country): UIO[Unit] =
    ZIO.logInfo("Caching country.") *>
      self.ref.update(_ + (country.name -> country)) @@ ZIOAspect.annotated("countryName", country.name)

  override def initCountryCache(): UIO[Unit] = {
    ZStream.fromIterable(CountryGenerator.countries).mapZIO( country =>
      self.ref.update(_ + (country.name -> country)) @@ ZIOAspect.annotated("countryName", country.name)
      ).runDrain
  }
}

object CountryCacheLive {
  lazy val layer: ULayer[CountryCache] =
    ZLayer {
      Ref.make(Map.empty[String, Country]).map(CountryCacheLive(_))
    }
}
