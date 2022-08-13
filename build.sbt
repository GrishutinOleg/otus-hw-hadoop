ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "otus-hw-hadoop"
  )

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "3.2.2"
)