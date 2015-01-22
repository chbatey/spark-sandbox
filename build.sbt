import _root_.sbtassembly.Plugin.AssemblyKeys
import AssemblyKeys._
import _root_.sbtassembly.Plugin.AssemblyKeys._
import _root_.sbtassembly.Plugin._

assemblySettings

scalaVersion := "2.11.5"

name := "SparkJobs"

version := "1.0"

val sparkVersion = "1.2.0"
val cassandraVersion = "2.1.2"

//libraryDependencies += "com.google.guava" % "guava" % "16.0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion  % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.2.0-alpha1"
//libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.4" force()

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11"

libraryDependencies += "org.json4s" %% "json4s-ext" % "3.2.11"

libraryDependencies += "org.apache.cassandra" % "cassandra-thrift" % cassandraVersion

libraryDependencies += "org.apache.cassandra" % "cassandra-clientutil" % cassandraVersion

libraryDependencies += "joda-time" % "joda-time" % "2.5"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"

jarName in assembly := "SparkSandbox.jar"

assemblyOption in assembly ~= {
  _.copy(includeScala = false)
}

