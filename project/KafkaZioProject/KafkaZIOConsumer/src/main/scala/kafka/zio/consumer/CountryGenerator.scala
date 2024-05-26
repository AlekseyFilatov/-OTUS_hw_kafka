package kafka.zio.consumer


  object CountryGenerator {
    lazy val countries = List(
      Country("Sweden", "capital", "region", "subregion", 123),
      Country("Austria", "capital", "region", "subregion", 123),
      Country("Belgium", "capital", "region", "subregion", 123),
      Country("Germany", "capital", "region", "subregion", 123),
      Country("Poland", "capital", "region", "subregion", 123),
      Country("Serbia", "capital", "region", "subregion", 123)
    )
  }


