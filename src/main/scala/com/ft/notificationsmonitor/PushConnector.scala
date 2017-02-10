package com.ft.notificationsmonitor

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.ft.notificationsmonitor.PushConnector.{Connect, StreamEnded}
import com.ft.notificationsmonitor.PushReader.{CancelStreams, Read}
import com.ft.notificationsmonitor.model.HttpConfig
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class PushConnector(private val httpConfig: HttpConfig,
                    private val pairMatcher: ActorRef) extends Actor {

  implicit private val sys = context.system
  implicit private val ec = context.dispatcher
  implicit private val mat = ActorMaterializer()

  private val logger = LoggerFactory.getLogger(getClass)
  private val connectionFlow = Http().outgoingConnectionHttps(httpConfig.hostname, httpConfig.port)
  private var reader = context.actorOf(PushReader.props(pairMatcher))
  private var cancelStreams = false

  override def receive: Receive = {
    case Connect =>
      val request = HttpRequest(uri = httpConfig.uri)
        .addHeader(Authorization(BasicHttpCredentials(httpConfig.credentials._1, httpConfig.credentials._2)))
      val responseF = Source.single(request)
        .via(connectionFlow)
        .runWith(Sink.head)
      responseF onComplete {
        case Failure(exception) =>
          logger.warn("Failed request. Retrying in a few moments... host={} uri={}", Array(httpConfig.hostname, httpConfig.uri, exception):_*)
          context.system.scheduler.scheduleOnce(5 seconds, self, Connect)
        case Success(response) =>
          if (!response.status.equals(StatusCodes.OK)) {
            logger.warn("Response status not ok. Retrying in a few moments... host={} uri={} status={}", Array(httpConfig.hostname, httpConfig.uri, response.status.intValue().toString):_*)
            context.system.scheduler.scheduleOnce(5 seconds, self, Connect)
          } else {
            logger.info("Connected to push feed. host={} uri={} status={}", Array(httpConfig.hostname, httpConfig.uri, response.status.intValue().toString):_*)
            reader = context.actorOf(PushReader.props(pairMatcher))
            reader ! Read(response.entity.dataBytes)
          }
      }

    case CancelStreams =>
      cancelStreams = true
      reader ! CancelStreams

    case StreamEnded =>
      if (!cancelStreams) {
        self ! Connect
      }
  }
}

object PushConnector {

  def props(httpConfig: HttpConfig, pairMatcher: ActorRef) = Props(new PushConnector(httpConfig, pairMatcher))

  case object Connect

  case object StreamEnded
}
