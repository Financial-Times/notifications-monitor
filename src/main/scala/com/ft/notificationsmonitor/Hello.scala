package com.ft.notificationsmonitor

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpHeader, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import org.slf4j.LoggerFactory
import akka.http.scaladsl.model.headers._
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Hello extends App {

  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val sys = ActorSystem("notifications-monitor")
  implicit private val mat = ActorMaterializer(ActorMaterializerSettings(sys).withDebugLogging(true))

  import sys.dispatcher

  val sensitiveConfig = ConfigFactory.parseFile(new File("src/main/resources/.sensitive.conf"))
  ConfigFactory.load().withFallback(sensitiveConfig)
  val username = sensitiveConfig.getString("basic-auth.username")
  val password = sensitiveConfig.getString("basic-auth.password")

  logger.info(username)
  logger.info(password)

  val connectionFlow = Http().outgoingConnectionHttps("pre-prod-uk-up.ft.com")
  val request = HttpRequest(uri = "/content/notifications-push").addHeader(Authorization(BasicHttpCredentials(username, password)))
  val responseFuture = Source.single(request)
    .via(connectionFlow)
    .runWith(Sink.head)
  responseFuture.andThen {
    case Success(resp) =>
      logger.info(resp.status.value)
      Await.ready(resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(body => logger.info(body.utf8String)), 10 seconds)
    case Failure(_) => logger.warn("request failed")
  }.andThen {
    case _ =>
      Await.ready(Http().shutdownAllConnectionPools()
        .flatMap(_ => sys.terminate()), 5 seconds)
      logger.info("terminated")
  }
}
