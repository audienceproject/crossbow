organization := "com.audienceproject"

name := "crossbow"

version := "0.1.5"

scalaVersion := "2.13.6"
crossScalaVersions := Seq(scalaVersion.value, "2.12.12", "2.11.12")

scalacOptions ++= Seq("-deprecation", "-feature", "-language:existentials")

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.0" % "test"

/**
 * Maven specific settings for publishing to Maven central.
 */
publishMavenStyle := true
Test / publishArtifact := false
pomIncludeRepository := { _ => false }
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
pomExtra := <url>https://github.com/audienceproject/crossbow</url>
  <licenses>
    <license>
      <name>MIT License</name>
      <url>https://opensource.org/licenses/MIT</url>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:audienceproject/crossbow.git</url>
    <connection>scm:git:git//github.com/audienceproject/crossbow.git</connection>
    <developerConnection>scm:git:ssh://github.com:audienceproject/crossbow.git</developerConnection>
  </scm>
  <developers>
    <developer>
      <id>jacobfi</id>
      <name>Jacob Fischer</name>
      <email>jacob.fischer@audienceproject.com</email>
      <organization>AudienceProject</organization>
      <organizationUrl>https://www.audienceproject.com</organizationUrl>
    </developer>
  </developers>
