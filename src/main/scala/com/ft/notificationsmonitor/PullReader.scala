package com.ft.notificationsmonitor

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.ActorMaterializer
import com.ft.notificationsmonitor.PullReader.Read
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

class PullReader extends Actor {

  private val logger = LoggerFactory.getLogger(getClass)

  implicit private val ec = context.dispatcher
  implicit private val mat = ActorMaterializer()

  override def receive: Receive = {
    case Read(entity) =>
      readBody(entity)
  }

  private def readBody(entity: ResponseEntity) = {
    entity.toStrict(5 seconds)
      .map(e => logger.info(e.data.utf8String))
    Unit
  }
}

object PullReader {

  def props = Props(new PullReader())

  case class Read(entity: ResponseEntity)
}
