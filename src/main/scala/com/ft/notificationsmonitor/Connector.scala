package com.ft.notificationsmonitor

import akka.actor.{Actor, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

class Connector(private val credentials: (String, String)) extends Actor {

  implicit private val sys = context.system
  implicit private val ec = context.dispatcher
  implicit private val mat = ActorMaterializer()

  private val logger = LoggerFactory.getLogger(getClass)
  private val connectionFlow = Http().outgoingConnectionHttps("pre-prod-uk-up.ft.com")
  private val reader = context.actorOf(Reader.props)

  override def receive: Receive = {
    case Connect(uriToConnect) =>
      val request = HttpRequest(uri = uriToConnect)
        .addHeader(Authorization(BasicHttpCredentials(credentials._1, credentials._2)))
      val responseF = Source.single(request)
        .via(connectionFlow)
        .runWith(Sink.head)
      responseF onComplete {
        case Failure(exception) => logger.warn("Failed request", exception)
        case Success(response) =>
          logger.info(response.status.value)
          reader ! Read(response.entity.dataBytes)
      }

    case CancelStreams =>
      reader ! CancelStreams
  }
}

object Connector {

  def props(credentials: (String, String)) = Props(new Connector(credentials))
}

case class Connect(uri: String)
