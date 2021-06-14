import sbt._

object Dependencies {

  val munitVersion = "0.7.26"

  private lazy val test = Seq(
    "org.scalameta" %% "munit" % munitVersion
  )

  lazy val all : Seq[ModuleID] = test.map(_ % Test) ++ test.map(_ % IntegrationTest)

  lazy val overrides : Seq[ModuleID] = Seq()
}
