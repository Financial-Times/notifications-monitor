package com.ft.notificationsmonitor

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.Await

object Hello extends App {

  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val sys = ActorSystem("notifications-monitor")

  import sys.dispatcher

  private val sensitiveConfig = ConfigFactory.parseFile(new File("src/main/resources/.sensitive.conf"))
  ConfigFactory.load().withFallback(sensitiveConfig)

  scala.sys addShutdownHook shutdown

  val (username, password) = (sensitiveConfig.getString("basic-auth.username"),
    sensitiveConfig.getString("basic-auth.password"))

  val connector = sys.actorOf(Connector.props((username, password)))

  connector ! Connect("/content/notifications-push")

  private def shutdown() = {
    logger.info("Exiting...")
    connector ! CancelStreams
    Await.ready(
      Http().shutdownAllConnectionPools()
        .flatMap(_ => sys.terminate()), 5 seconds
    )
  }
}
