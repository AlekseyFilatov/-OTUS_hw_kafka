package kafka.zio.producer

import zio.{Exit, Task, ZIO}

import java.io.IOException
import java.nio.file.Paths

object EventGenerator {

  val workingDir: String = Paths.get(".").toAbsolutePath.toString.replace(".", "")

  private def readFile(file: String): ZIO[String, IOException, String] = {
    lazy val fileReader = ZIO.fromAutoCloseable(
      ZIO.attemptBlockingIO(scala.io.Source.fromFile(s"$workingDir$file")))
    lazy val read = ZIO.scoped {
      fileReader.map(_.getLines().toList.mkString("\n"))
    }
    read
   }

  def logFailures[R, E, A](zio: ZIO[R, E, A]): ZIO[R, E, A] =
    zio.foldCauseZIO(
      cause => {
        cause.prettyPrint
        zio
      },
      _ => zio
    )

  def readFileZio(file: String): ZIO[String, Nothing, String] =
    logFailures {
      readFile(file).foldZIO(
        error => ZIO.succeed(s"Ошибка - $error"),
        data => ZIO.succeed(data)
      )
    }

  def StringField: MonadParser[String, String] = MonadParser[String, String] {
    case str =>
      val idx = str.indexOf(";")
      if (idx > -1)
        (str.substring(0, idx), str.substring(idx+1))
      else
        (str, "")
  }

  def IntField: MonadParser[Int, String] = StringField.map(_.toInt)
  def DoubleField: MonadParser[Double, String] = StringField.map(_.toDouble)
  def LongField: MonadParser[Long, String] = StringField.map(_.toLong)
  def BooleanField: MonadParser[Boolean, String] = StringField.map(_.toBoolean)

  lazy val parser: MonadParser[TransactionRaw, String] = for {
    userId <- LongField
    country <- StringField
    amount <- DoubleField
  } yield TransactionRaw(userId, country, BigDecimal(amount))

  lazy val transactions = List(
    TransactionRaw(1, "Serbia", BigDecimal(12.99)),
    TransactionRaw(1, "Serbia", BigDecimal(23.99)),
    TransactionRaw(1, "Serbia", BigDecimal(11.99)),
    TransactionRaw(2, "Serbia", BigDecimal(24.99)),
    TransactionRaw(2, "Serbia", BigDecimal(31.99)),
    TransactionRaw(3, "Poland", BigDecimal(99.99)),
    TransactionRaw(3, "Poland", BigDecimal(11.99)),
    TransactionRaw(3, "Poland", BigDecimal(99.99)),
    TransactionRaw(3, "Poland", BigDecimal(11.99)),
    TransactionRaw(4, "Poland", BigDecimal(22.99)),
    TransactionRaw(5, "Germany", BigDecimal(22.99)),
    TransactionRaw(5, "Germany", BigDecimal(69.99)),
    TransactionRaw(6, "Belgium", BigDecimal(22.99)),
    TransactionRaw(7, "Austria", BigDecimal(99.99)),
    TransactionRaw(7, "Austria", BigDecimal(99.99)),
    TransactionRaw(7, "Austria", BigDecimal(99.99)),
    TransactionRaw(8, "Austria", BigDecimal(99.99)),
    TransactionRaw(9, "Sweden", BigDecimal(123.99)),
    TransactionRaw(10, "Sweden", BigDecimal(100.00)),
    TransactionRaw(10, "Sweden", BigDecimal(200.00)),
    TransactionRaw(10, "Sweden", BigDecimal(100.00))
  )
}