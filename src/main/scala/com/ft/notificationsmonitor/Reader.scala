package com.ft.notificationsmonitor

import akka.Done
import akka.actor.{Actor, PoisonPill, Props}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, KillSwitches}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import org.slf4j.LoggerFactory

import scala.concurrent.Promise
import scala.util.Success

class Reader extends Actor {

  private val logger = LoggerFactory.getLogger(getClass)

  implicit private val ec = context.dispatcher
  implicit private val mat = ActorMaterializer()

  private val killPromise = Promise[Done]()

  override def receive: Receive = {
    case Read(source) =>
      logger.debug("Read(source)")
      val killSwitch = consumeBodyByPrinting(source)
      killPromise.future.onComplete { _ =>
        killSwitch.shutdown()
        logger.debug("killing myself")
        self ! PoisonPill
      }
      logger.debug("after Read(source) actor ready for other messages")

    case CancelStreams =>
      killPromise.complete(Success(Done))
  }

  private def consumeBodyByPrinting(body: Source[ByteString, Any]) = {
    body.viaMat(KillSwitches.single)(Keep.right)
      .fold(ByteString("")){ (acc, next) =>
        val nextString = next.utf8String
        nextString.lastIndexOf("\n\n") match {
          case -1 =>
            System.out.print(nextString + "|")
            acc ++ next
          case i =>
            val lines = nextString.split("\n\n").toList
            if (nextString.length - 2 == i) {
              lines.map(_.stripPrefix("data: [").stripSuffix("]")).foreach {
                case "" => logger.info("heartbeat")
                case s => logger.info(s)
              }
              ByteString("")
            } else {
              lines.dropRight(1).foreach(l => logger.info(l))
              ByteString(lines.last)
            }
        }
      }
      .toMat(Sink.ignore)(Keep.left)
      .run()
  }
}

object Reader {

  def props = Props(new Reader())
}

case class Read(body: Source[ByteString, Any])

case object CancelStreams
