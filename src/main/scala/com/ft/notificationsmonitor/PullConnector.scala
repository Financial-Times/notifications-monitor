package com.ft.notificationsmonitor

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.ft.notificationsmonitor.PullConnector.RequestSinceLast
import com.ft.notificationsmonitor.model.{DatedPullEntry, HttpConfig, PullPage}
import com.ft.notificationsmonitor.model.PullPageFormat._
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class PullConnector(private val httpConfig: HttpConfig,
                    private val pairMatcher: ActorRef) extends Actor with ActorLogging {

  implicit private val sys = context.system
  implicit private val ec = context.dispatcher
  implicit private val mat = ActorMaterializer()

  private val connectionFlow = Http().outgoingConnectionHttps(httpConfig.hostname, httpConfig.port)
  private var last = ZonedDateTime.now()

  override def receive: Receive = {
    case RequestSinceLast =>
      makeRequest(last)
  }

  private def makeRequest(date: ZonedDateTime) = {
    val uri1 = httpConfig.uri + "?since=" + date.format(DateTimeFormatter.ISO_INSTANT)
    val request = HttpRequest(uri = uri1)
      .addHeader(Authorization(BasicHttpCredentials(httpConfig.credentials._1, httpConfig.credentials._2)))
    val responseF = Source.single(request)
      .via(connectionFlow)
      .runWith(Sink.head)
    responseF.onComplete{
      case Failure(exception) =>
        log.error(exception, "Failed request. host={} uri={}", httpConfig.hostname, httpConfig.uri)
      case Success(response) =>
        if (!response.status.equals(StatusCodes.OK)) {
          log.warning("Response status not ok. Retrying in a few moments... host={} uri={} status={}", httpConfig.hostname, httpConfig.uri, response.status.intValue)
        } else {
          last = ZonedDateTime.now()
          response.entity.toStrict(5 seconds).map(httpEntity => parsePage(httpEntity.data.utf8String))
        }
    }
  }

  private def parsePage(pageText: String) = {
    Future {
      val jsonPage = pageText.parseJson
      jsonPage.convertTo[PullPage]
    }.onComplete {
      case Success(page) =>
        page.notifications.foreach { entry =>
          log.info(entry.id)
          pairMatcher ! DatedPullEntry(entry, ZonedDateTime.now())
        }
      case Failure(t) => log.error(t, "Error deserializing notifications response: {}", pageText)
    }
  }
}

object PullConnector {

  def props(httpConfig: HttpConfig, pairMatcher: ActorRef) =
    Props(new PullConnector(httpConfig, pairMatcher))

  case object RequestSinceLast
}
