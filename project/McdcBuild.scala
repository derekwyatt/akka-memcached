import sbt._
import Keys._

object McdcBuild extends Build {
  lazy val mcdc = Project("mcdc", file("."))
            .configs(IntegrationTest)
            .settings(Defaults.itSettings:_*)
            .settings(libraryDependencies += actor)
            .settings(libraryDependencies += testkit)
            .settings(libraryDependencies += scalatest)

  val actor = "com.typesafe.akka" %% "akka-actor" % "2.2.3"
  val testkit = "com.typesafe.akka" %% "akka-testkit" % "2.2.3" % "it,test"
  val scalatest = "org.scalatest" %% "scalatest" % "2.0" % "it,test"
}
