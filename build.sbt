val _repoName = "avro-data-generator"
val _githubOwner = "andrewinci"
val _organization = "com.github.andrewinci"

lazy val publishSettings = Seq(
  githubOwner := _githubOwner,
  githubRepository := _repoName,
  githubTokenSource := TokenSource.GitConfig("github.token") || TokenSource.Environment("GITHUB_TOKEN")
)

lazy val default = Seq(
  organization := _organization,
  scalaVersion := "2.12.13",
  versionScheme := Some("semver-spec"),
  version := "0.1.0-SNAPSHOT",
  testFrameworks += new TestFramework("munit.Framework"),
  resolvers ++= Resolvers.all
)

lazy val root = Project(_repoName, file("."))
  .settings(default)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(libraryDependencies ++= Dependencies.all)
  .settings(dependencyOverrides ++= Dependencies.overrides)
  .settings(testFrameworks += new TestFramework("munit.Framework"))
  .settings((Compile / compile) := ((Compile / compile) dependsOn scalafmtCheckAll).value)
  .settings(publishSettings)
  .settings(
    scalacOptions ++= Seq(
      "-Ypartial-unification",
      "-unchecked",
      "-deprecation",
      "-feature",
      "-Xfatal-warnings",
      "-language:higherKinds"
    )
  )
