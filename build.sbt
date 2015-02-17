import _root_.sbtassembly.Plugin.AssemblyKeys
import AssemblyKeys._
import _root_.sbtassembly.Plugin.AssemblyKeys._
import _root_.sbtassembly.Plugin._

assemblySettings

scalaVersion := "2.10.4"

name := "SparkJobs"

version := "1.0"

val sparkVersion = "1.2.0"
val cassandraVersion = "2.1.2"
val connectorVersion = "1.2.0-alpha1"
val json4sVersion = "3.2.11"
//libraryDependencies += "com.google.guava" % "guava" % "16.0.1"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
//
//libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion  % "provided"
//
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion  % "provided"
//
//libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % connectorVersion

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector-embedded_2.10" % connectorVersion

libraryDependencies += "org.json4s" %% "json4s-jackson" % json4sVersion

libraryDependencies += "org.json4s" %% "json4s-ext" % json4sVersion

libraryDependencies += "org.apache.cassandra" % "cassandra-thrift" % cassandraVersion

libraryDependencies += "org.apache.cassandra" % "cassandra-clientutil" % cassandraVersion

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.34"

libraryDependencies += "joda-time" % "joda-time" % "2.5"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.0" % "test"

libraryDependencies += "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.1"

//libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

jarName in assembly := "SparkSandbox.jar"

assemblyOption in assembly ~= {
  _.copy(includeScala = false)
}

