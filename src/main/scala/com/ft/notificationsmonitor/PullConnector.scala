package com.ft.notificationsmonitor

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import akka.actor.{Actor, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.ft.notificationsmonitor.PullConnector.RequestSinceLast
import com.ft.notificationsmonitor.PullReader.Read
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

class PullConnector(private val hostname: String,
                    private val port: Int,
                    private val uriToConnect: String,
                    private val credentials: (String, String)) extends Actor {

  implicit private val sys = context.system
  implicit private val ec = context.dispatcher
  implicit private val mat = ActorMaterializer()

  private val logger = LoggerFactory.getLogger(getClass)
  private val connectionFlow = Http().outgoingConnectionHttps(hostname, port)
  private val reader = context.actorOf(PullReader.props, "pull-reader-" + ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT))
  private var last = ZonedDateTime.now()

  override def receive: Receive = {
    case RequestSinceLast =>
      makeRequest(last)
  }

  private def makeRequest(date: ZonedDateTime): Unit = {
    val uri1 = uriToConnect + "?since=" + date.format(DateTimeFormatter.ISO_INSTANT)
    val request = HttpRequest(uri = uri1)
      .addHeader(Authorization(BasicHttpCredentials(credentials._1, credentials._2)))
    val responseF = Source.single(request)
      .via(connectionFlow)
      .runWith(Sink.head)
    responseF.onComplete{
      case Failure(exception) =>
        logger.warn("Failed request. host={} uri={}", Array(hostname, uriToConnect, exception):_*)
      case Success(response) =>
        if (!response.status.equals(StatusCodes.OK)) {
          logger.warn("Response status not ok. Retrying in a few moments... host={} uri={} status={}", Array(hostname, uriToConnect, response.status.intValue.toString):_*)
        } else {
          last = ZonedDateTime.now()
          reader ! Read(response.entity)
        }
    }
  }
}

object PullConnector {

  def props(hostname: String, port: Int, uri: String, credentials: (String, String)) =
    Props(new PullConnector(hostname, port, uri, credentials))

  case object RequestSinceLast
}
