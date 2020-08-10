name := "crossbow"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= {
  val arrowVersion = "1.0.0"
  Seq(
    "org.apache.arrow" % "arrow-java-root" % arrowVersion,
    "org.apache.arrow" % "arrow-algorithm" % arrowVersion,
    "org.apache.arrow" % "arrow-vector" % arrowVersion,
    "org.apache.arrow" % "arrow-memory" % arrowVersion,
    "org.apache.arrow" % "arrow-memory-unsafe" % arrowVersion
  )
}
