package com.ft.notificationsmonitor

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.util.{Failure, Success}
import scala.concurrent.duration._

object Hello extends App {

  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val sys = ActorSystem("notifications-monitor")
  implicit private val mat = ActorMaterializer()
  import sys.dispatcher

  private val http = Http()
  private val reqF = http.singleRequest(HttpRequest(uri = "http://akka.io"))

  http.singleRequest(HttpRequest(uri = "http://akka.io")).onComplete {
      case Success(resp) =>
        resp match {
          case HttpResponse(StatusCodes.OK, headers, entity, _) =>
            entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach { body =>
              logger.info("Got response, body: " + body.utf8String)
            }
          case resp @ HttpResponse(code, _, _, _) =>
            logger.info("Request failed, response code: " + code)
            resp.discardEntityBytes()
        }
      case Failure(t) =>
        logger.warn("fail")
    }

  Await.ready(
    reqF.flatMap(_ => sys.terminate().
      map(_ => logger.info("Bye, world!"))
    ), 2 seconds)
}
