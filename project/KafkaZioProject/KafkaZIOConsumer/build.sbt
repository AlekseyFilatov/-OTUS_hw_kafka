ThisBuild / scalaVersion     := "2.13.14"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "KafkaZIOConsumer",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.12",
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.7.0",
    libraryDependencies += "org.apache.kafka" % "kafka-streams" % "3.7.0",
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.13",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.21",
      "dev.zio" %% "zio-test" % "2.0.21" % Test,
      "dev.zio" %% "zio-logging" % "2.1.16",
      "dev.zio" %% "zio-logging-slf4j" % "2.1.16",
      "dev.zio" %% "zio-jdbc" % "0.1.2",
      "dev.zio" %% "zio-sql" % "0.1.2",
      "dev.zio" %% "zio-schema" % "0.4.17",
      "dev.zio" %% "zio-kafka" % "2.7.3",
      "dev.zio" %% "zio-http" % "3.0.0-RC2",
      "dev.zio" %% "zio-macros" % "2.0.15",
      "dev.zio" %% "zio-json" % "0.6.2",
      "dev.zio" %% "zio-streams" % "2.0.15",
      "dev.zio" %% "zio-config-magnolia" % "4.0.0-RC16",
      "dev.zio" %% "zio-config-typesafe"  % "4.0.0-RC16",
      "org.scalatest" %% "scalatest" % "3.2.18"
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
