package com.ft.notificationsmonitor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.ActorMaterializer
import com.ft.notificationsmonitor.PullReader.Read
import spray.json.DefaultJsonProtocol._
import spray.json._
import PullPageFormat._
import PullEntryFormat._
import LinkFormat._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class PullReader(private val pairMatcher: ActorRef) extends Actor with ActorLogging {

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
      case Success(page) =>
        page.notifications.foreach { entry =>
          log.info(entry.id)
          pairMatcher ! entry
        }
      case Failure(t) => log.error(t, "Error deserializing notifications response: {}", pageText)
    }
  }
}

object PullReader {

  def props(pairMatcher: ActorRef) = Props(new PullReader(pairMatcher))

  case class Read(entity: ResponseEntity)
}

case class PullPage(requestUrl: String, notifications: List[PullEntry], links: List[Link])

case class Link(href: String, rel: String)

object PullPageFormat {

  implicit val pullPageFormat: RootJsonFormat[PullPage] = DefaultJsonProtocol.jsonFormat3(PullPage)

}

object LinkFormat {

  implicit val linkFormat: RootJsonFormat[Link] = DefaultJsonProtocol.jsonFormat2(Link)
}
