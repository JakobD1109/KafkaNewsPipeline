name := "kafka-news-pipeline-scala"

version := "0.4"
scalaVersion := "2.12.18"

resolvers += Resolver.mavenCentral

libraryDependencies ++= Seq(
  // Kafka
  "org.apache.kafka" % "kafka-clients" % "3.5.0",

  // JSON - Jackson
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.15.2",

  // Config
  "com.typesafe" % "config" % "1.4.2",

  // Slick & PostgreSQL
  "com.typesafe.slick" %% "slick" % "3.4.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.4.1",
  "org.postgresql" % "postgresql" % "42.6.0",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.8",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.slf4j" % "slf4j-api" % "2.0.7"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked", "-deprecation", "-feature",
  "-language:existentials", "-language:higherKinds", "-language:implicitConversions"
)

javaOptions ++= Seq(
  "-Xms256m",
  "-Xmx1024m",
  "-XX:+UseG1GC"
)