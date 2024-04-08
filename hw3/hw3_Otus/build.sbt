ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "objectKafka",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.12",
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.7.0",
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.13"
  )

