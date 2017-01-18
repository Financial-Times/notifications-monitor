package com.ft.notificationsmonitor

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Hello extends App {

  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val sys = ActorSystem("notifications-monitor")
  implicit private val mat = ActorMaterializer(ActorMaterializerSettings(sys).withDebugLogging(true))

  import sys.dispatcher

  val connectionFlow = Http().outgoingConnectionHttps("getbootstrap.com")
  val responseFuture = Source.single(HttpRequest(uri = "/"))
    .via(connectionFlow)
    .runWith(Sink.head)

  responseFuture.andThen {
    case Success(resp) =>
      Await.ready(resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(body => logger.info(body.utf8String)), 10 seconds)
    case Failure(_) => logger.warn("request failed")
  }.andThen {
    case _ =>
      Await.ready(Http().shutdownAllConnectionPools()
        .flatMap(_ => sys.terminate()), 5 seconds)
      logger.info("terminated")
  }
}
