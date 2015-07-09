scalaVersion := "2.11.7"

name := "SparkJobs"

version := "1.0"

val sparkVersion = "1.4.0"
val connectorVersion = "1.4.0-M1"
val json4sVersion = "3.2.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % connectorVersion,
  "com.datastax.spark" %% "spark-cassandra-connector-embedded" % connectorVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "org.json4s" %% "json4s-ext" % json4sVersion,
  "mysql" % "mysql-connector-java" % "5.1.34",
  "joda-time" % "joda-time" % "2.5",
  "org.apache.commons" % "commons-lang3" % "3.3.2",
  "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.1",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2")
