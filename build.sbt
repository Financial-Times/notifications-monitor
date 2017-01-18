name := """notifications-monitor"""

version := "1.0.0"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.16",
  "com.typesafe.akka" %% "akka-http-core" % "10.0.2",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.2",
  "org.slf4j" % "slf4j-api" % "1.7.22",
  "ch.qos.logback" % "logback-classic" % "1.1.8"
  )
