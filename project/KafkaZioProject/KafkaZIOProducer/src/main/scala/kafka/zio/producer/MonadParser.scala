package kafka.zio.producer

class MonadParser[T, Src](private val p: Src => (T, Src)) {
  def flatMap[M](f: T => MonadParser[M, Src]): MonadParser[M, Src] =
    MonadParser { src =>
      val (word, rest) = p(src)
      val mn = f(word)
      val res = mn.p(rest)
      res
    }



  def map[M](f: T=>M): MonadParser[M, Src] =
    MonadParser{ src =>
      val (word, rest) = p(src)
      (f(word), rest)
    }

  def parse(src: Src): T = p(src)._1
}

object MonadParser{
  def apply[T, Src](f: Src => (T, Src)) = new MonadParser[T, Src](f)
}
