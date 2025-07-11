name := "kafka-news-pipeline-scala"

version := "0.4"

scalaVersion := "2.12.18"

// Resolver for Maven Central (explicit)
resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

libraryDependencies ++= Seq(
  // Kafka
  "org.apache.kafka" % "kafka-clients" % "3.5.0",
  
  // JSON parsing - Jackson for producer
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
  
  // Database - Slick (for consumer)
  "com.typesafe.slick" %% "slick" % "3.4.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.4.1",
  "org.postgresql" % "postgresql" % "42.6.0",
  
  // JSON parsing - Circe (for consumer)
  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-generic" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5",
  
  // Configuration
  "com.typesafe" % "config" % "1.4.2",
  
  // Scala Standard Library Extensions
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
  
  // Async/Concurrent Support
  "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
  
  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.8",
  "ch.qos.logback" % "logback-core" % "1.4.8",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.slf4j" % "slf4j-api" % "2.0.7"
)

// Scala compiler options
scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions"
)

// JVM options
javaOptions ++= Seq(
  "-Xms256m",
  "-Xmx1024m",
  "-XX:+UseG1GC"
)

// Fork settings for running
fork := true

// Main class settings (optional - helps with sbt run)
Compile / mainClass := Some("NewsProducerApp")

// Test settings
Test / parallelExecution := false

// Source directories
Compile / scalaSource := baseDirectory.value / "src" / "main" / "scala"
Test / scalaSource := baseDirectory.value / "src" / "test" / "scala"

// Resource directories
Compile / resourceDirectory := baseDirectory.value / "src" / "main" / "resources"
Test / resourceDirectory := baseDirectory.value / "src" / "test" / "resources"

// Clean up target directory on clean
cleanFiles += baseDirectory.value / "logs"