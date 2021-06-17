import sbt._

object Dependencies {

  private val munitVersion = "0.7.26"
  private val avroVersion = "1.10.2"
  private val circeVersion = "0.14.1"

  private lazy val munit = Seq(
    "org.scalameta" %% "munit" % munitVersion
  )

  private lazy val avro = Seq(
    "org.apache.avro" % "avro" % avroVersion
  )

  private lazy val cats = Seq(
    "org.typelevel" %% "cats-core" % "2.3.0"
  )

  lazy val main: Seq[ModuleID] = avro ++ cats
  lazy val test: Seq[ModuleID] = munit.map(_ % Test) ++ munit.map(_ % IntegrationTest)
  lazy val it: Seq[ModuleID] = munit.map(_ % IntegrationTest)
  lazy val all: Seq[ModuleID] = main ++ test ++ it

  lazy val overrides: Seq[ModuleID] = Seq()
}
