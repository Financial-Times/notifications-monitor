package com.ft.notificationsmonitor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.ActorMaterializer
import com.ft.notificationsmonitor.PullReader.Read
import com.ft.notificationsmonitor.model.PullPage
import com.ft.notificationsmonitor.model.PullPageFormat._
import spray.json.DefaultJsonProtocol._
import spray.json._

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

