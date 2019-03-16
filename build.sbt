name := "Sample Analytics"

import Resolvers._

version := "1.0"

autoScalaLibrary := false
scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq( 
"org.apache.spark" %% "spark-core" % sparkVersion,
"org.apache.spark" %% "spark-sql" % sparkVersion,
"org.apache.spark" %% "spark-graphx" % sparkVersion,
"org.apache.spark" %% "spark-streaming" % sparkVersion)
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.7.0" % "provided"
libraryDependencies += "spark.jobserver" %% "job-server-extras" % "0.7.0" % "provided"
resolvers += "Oracle Repository" at "http://download.oracle.com/maven"
resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"








