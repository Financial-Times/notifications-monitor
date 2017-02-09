package com.ft.notificationsmonitor

import akka.actor.{Actor, ActorLogging, Props}
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.ActorMaterializer
import com.ft.notificationsmonitor.PullReader.Read
import spray.json.DefaultJsonProtocol._
import spray.json._
import PullPageFormat._
import NotificationEntryFormat._
import LinkFormat._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class PullReader extends Actor with ActorLogging {

  implicit private val ec = context.dispatcher
  implicit private val mat = ActorMaterializer()

  override def receive: Receive = {
    case Read(entity) =>
      readBody(entity)
  }

  private def readBody(entity: ResponseEntity) = {
    entity.toStrict(5 seconds)
      .map(httpEntity => parsePage(httpEntity.data.utf8String))
    Unit
  }

  private def parsePage(pageText: String) = {
    Future {
      val jsonPage = pageText.parseJson
      jsonPage.convertTo[PullPage]
    }.onComplete {
      case Success(page) => page.notifications.foreach(entry => log.info(entry.id))
      case Failure(t) => log.error(t, "Error deserializing notifications response")
    }
  }
}

object PullReader {

  def props = Props(new PullReader())

  case class Read(entity: ResponseEntity)
}

case class PullPage(requestUrl: String, notifications: List[NotificationEntry], links: List[Link])

object PullPageFormat {

  implicit val pullPageFormat: RootJsonFormat[PullPage] = DefaultJsonProtocol.jsonFormat3(PullPage)
}

case class Link(href: String, rel: String)

object LinkFormat {

  implicit val linkFormat: RootJsonFormat[Link] = DefaultJsonProtocol.jsonFormat2(Link)
}