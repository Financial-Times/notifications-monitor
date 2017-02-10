package com.ft.notificationsmonitor

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import com.ft.notificationsmonitor.PairMatcher.Report
import com.ft.notificationsmonitor.PullConnector.RequestSinceLast
import com.ft.notificationsmonitor.PushConnector.Connect
import com.ft.notificationsmonitor.PushReader.CancelStreams
import com.ft.notificationsmonitor.model.HttpConfig
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.Await

object NotificationsMonitor extends App {

  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val sys = ActorSystem("notifications-monitor")
  import sys.dispatcher

  logger.info("Starting up...")

  private val sensitiveConfig = ConfigFactory.parseFile(new File("src/main/resources/.sensitive.conf"))
  private val config = ConfigFactory.load().withFallback(sensitiveConfig)
  private val (username, password) = (sensitiveConfig.getString("basic-auth.username"),
    sensitiveConfig.getString("basic-auth.password"))
  private val pairMatcher = sys.actorOf(PairMatcher.props)
  private val pushHttpConfig = HttpConfig(config.getString("push-host"), config.getInt("push-port"),
    config.getString("push-uri"), (username, password))
  private val pushConnector = sys.actorOf(PushConnector.props(pushHttpConfig, pairMatcher))
  private val pullHttpConfig = HttpConfig(config.getString("pull-host"), config.getInt("pull-port"),
    config.getString("pull-uri"), (username, password))
  private val pullConnector = sys.actorOf(PullConnector.props(pullHttpConfig, pairMatcher))
  pushConnector ! Connect
  private val pullSchedule = sys.scheduler.schedule(0 seconds, 1 minute, pullConnector, RequestSinceLast)
  private val reportSchedule = sys.scheduler.schedule(0 seconds, 1 minute, pairMatcher, Report)
  scala.sys addShutdownHook shutdown

  private def shutdown() = {
    logger.info("Exiting...")
    pullSchedule.cancel()
    reportSchedule.cancel()
    pushConnector ! CancelStreams
    Await.ready(
      Http().shutdownAllConnectionPools()
        .flatMap(_ => sys.terminate()), 5 seconds
    )
  }
}
