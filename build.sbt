name := """notifications-monitor"""

version := "1.0.1-SNAPSHOT"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.16",
  "com.typesafe.akka" %% "akka-http-core" % "10.0.1",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.1",
  "com.typesafe.akka" %% "akka-slf4j" % "2.4.16",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.16" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.22",
  "ch.qos.logback" % "logback-classic" % "1.1.8",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.mockito" % "mockito-core" % "2.7.11" % "test"
)

mainClass in (Compile, run) := Some("com.ft.notificationsmonitor.NotificationsMonitor")

javaOptions in run += "-XX:+CMSClassUnloadingEnabled"

scalacOptions ++= Seq("-feature", "-language:postfixOps")

testOptions in Test := Seq(Tests.Argument(TestFrameworks.JUnit, "-q"))

crossPaths := false

enablePlugins(sbtdocker.DockerPlugin, JavaAppPackaging)

lazy val dockerSettings = Seq(
  dockerfile in docker := {
    val appDir: File = stage.value
    val targetDir = "/notifications-monitor"

    sbtdocker.immutable.Dockerfile.empty
      .from("openjdk:8-jdk-alpine")
      .copy(appDir, targetDir)
      .expose(8080)
      .entryPoint(s"$targetDir/bin/${executableScriptName.value}")
  }
)
