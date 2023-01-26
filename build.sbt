ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "1.0"
ThisBuild / organization := "com.sparkpsql"

lazy val root = (project in file(".")).settings(
	name := "sparkPostgres"
)

// https://mvnrepository.com/artifact (spark search)

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1" % "provided"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.15"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % "test"

libraryDependencies += "org.postgresql" % "postgresql" % "42.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.1" % "provided"

libraryDependencies ++= Seq(
    "com.typesafe" % "config" % "1.3.3"
)
