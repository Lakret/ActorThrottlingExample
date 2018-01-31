import sbt._

object Dependencies {
  private lazy val akkaVersion = "2.5.9"
  lazy val akkaActor = "com.typesafe.akka" %% "akka-actor" % akkaVersion
  lazy val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaVersion
  lazy val akkaContrib = "com.typesafe.akka" %% "akka-contrib" % akkaVersion

  // Test deps
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4" % Test
  lazy val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
  lazy val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test
}
