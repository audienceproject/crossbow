ThisBuild / organizationName := "AudienceProject"
ThisBuild / organizationHomepage := Some(url("https://audienceproject.com//"))
ThisBuild / homepage := Some(url("https://github.com/audienceproject/crossbow"))

ThisBuild / Test / publishArtifact := false

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/audienceproject/crossbow"),
    "scm:git@github.com/audienceproject/crossbow.git",
    Option("scm:git:ssh://github.com:audienceproject/crossbow.git")
  )
)
ThisBuild / developers := List(
  Developer(
    id = "jacobfi",
    name = "Jacob Fischer",
    email = "jacob.fischer@audienceproject.com",
    url = url("https://www.audienceproject.com")
  )
)

ThisBuild / licenses := List(
  "MIT" -> url("https://mit-license.org/")
)

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishMavenStyle := true

// new setting for the Central Portal
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}
