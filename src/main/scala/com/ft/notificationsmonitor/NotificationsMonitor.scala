package com.ft.notificationsmonitor

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import com.ft.notificationsmonitor.PullConnector.RequestSinceLast
import com.ft.notificationsmonitor.PushConnector.Connect
import com.ft.notificationsmonitor.PushReader.CancelStreams
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.Await

object NotificationsMonitor extends App {

  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val sys = ActorSystem("notifications-monitor")

  import sys.dispatcher

  private val sensitiveConfig = ConfigFactory.parseFile(new File("src/main/resources/.sensitive.conf"))
  ConfigFactory.load().withFallback(sensitiveConfig)

  scala.sys addShutdownHook shutdown

  private val (username, password) = (sensitiveConfig.getString("basic-auth.username"),
    sensitiveConfig.getString("basic-auth.password"))

  private val pushConnector = sys.actorOf(PushConnector.props("localhost", 8080, "/content/notifications-push", (username, password)))
  private val pullConnector = sys.actorOf(PullConnector.props("pre-prod-uk-up.ft.com", 443, "/content/notifications", (username, password)))

  pushConnector ! Connect
  private val pullSchedule = sys.scheduler.schedule(0 seconds, 10 seconds, pullConnector, RequestSinceLast)

  private def shutdown() = {
    logger.info("Exiting...")
    pullSchedule.cancel()
    pushConnector ! CancelStreams
    Await.ready(
      Http().shutdownAllConnectionPools()
        .flatMap(_ => sys.terminate()), 5 seconds
    )
  }
}
