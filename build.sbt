organization := "com.audienceproject"

name := "crossbow"

scalaVersion := "3.3.4"

scalacOptions ++= Seq("-deprecation", "-feature", "-language:implicitConversions")

libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.17" % "test"
