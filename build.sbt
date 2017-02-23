name := """notifications-monitor"""

version := "1.0.0"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.16",
  "com.typesafe.akka" %% "akka-http-core" % "10.0.1",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.1",
  "com.typesafe.akka" %% "akka-slf4j" % "2.4.16",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.16" % "test",
  "junit" % "junit" % "4.12" % "test",
  "org.mockito" % "mockito-core" % "2.7.11" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.22",
  "ch.qos.logback" % "logback-classic" % "1.1.8"
  )

mainClass in (Compile, run) := Some("com.ft.notificationsmonitor.NotificationsMonitor")

scalacOptions ++= Seq("-feature", "-language:postfixOps")

enablePlugins(JavaAppPackaging)
