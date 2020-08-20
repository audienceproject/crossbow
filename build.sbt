name := "crossbow"

version := "0.1"

scalaVersion := "2.13.3"

scalacOptions += "-deprecation"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.0" % "test"
