package com.ft.notificationsmonitor

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import akka.actor.{Actor, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.ft.notificationsmonitor.PushConnector.{Connect, StreamEnded}
import com.ft.notificationsmonitor.PushReader.{CancelStreams, Read}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class PushConnector(private val hostname: String,
                    private val port: Int,
                    private val uriToConnect: String,
                    private val credentials: (String, String)) extends Actor {

  implicit private val sys = context.system
  implicit private val ec = context.dispatcher
  implicit private val mat = ActorMaterializer()

  private val logger = LoggerFactory.getLogger(getClass)
  private val connectionFlow = Http().outgoingConnection(hostname, port)
  private var reader = context.actorOf(PushReader.props)

  override def receive: Receive = {
    case Connect =>
      val request = HttpRequest(uri = uriToConnect)
        .addHeader(Authorization(BasicHttpCredentials(credentials._1, credentials._2)))
      val responseF = Source.single(request)
        .via(connectionFlow)
        .runWith(Sink.head)
      responseF onComplete {
        case Failure(exception) =>
          logger.warn("Failed request. Retrying in a few moments...", exception)
          context.system.scheduler.scheduleOnce(5 seconds, self, Connect)
        case Success(response) =>
          logger.info("Connected to push feed. host=%s status=%d", response.status.value)
          if (!response.status.equals(StatusCodes.OK)) {
            logger.warn("Retrying in a few moments...")
            context.system.scheduler.scheduleOnce(5 seconds, self, Connect)
          } else {
            reader = context.actorOf(PushReader.props, "push-reader-" + ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT))
            reader ! Read(response.entity.dataBytes)
          }
      }

    case CancelStreams =>
      reader ! CancelStreams

    case StreamEnded =>
      self ! Connect
  }
}

object PushConnector {

  def props(hostname: String, port: Int, uri: String, credentials: (String, String)) =
    Props(new PushConnector(hostname, port, uri, credentials))

  case object Connect

  case object StreamEnded
}
