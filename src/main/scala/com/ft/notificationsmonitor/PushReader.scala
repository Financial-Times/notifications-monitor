package com.ft.notificationsmonitor

import akka.Done
import akka.actor.{Actor, PoisonPill, Props}
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import com.ft.notificationsmonitor.PushConnector.StreamEnded
import com.ft.notificationsmonitor.PushReader.Read
import org.slf4j.LoggerFactory

import scala.concurrent.Promise
import scala.util.Success

class PushReader extends Actor {

  private val logger = LoggerFactory.getLogger(getClass)

  implicit private val ec = context.dispatcher
  implicit private val mat = ActorMaterializer()

  private val willStopStreamP = Promise[Done]()

  override def receive: Receive = {
    case Read(source) =>
      val (killSwitch, doneF) = consumeBodyStream(source)

      willStopStreamP.future.onComplete { _ =>
        killSwitch.shutdown()
        self ! PoisonPill
      }

      doneF.onComplete { _ =>
        context.parent ! StreamEnded
      }

    case PushReader.CancelStreams =>
      willStopStreamP.complete(Success(Done))
  }

  private def consumeBodyStream(body: Source[ByteString, Any]) = {
    body.viaMat(KillSwitches.single)(Keep.right)
      .fold(ByteString(""))(foldPerLine)
      .toMat(Sink.ignore)(Keep.both)
      .run()
  }

  private def foldPerLine(acc: ByteString, next: ByteString) = {
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
}

object PushReader {

  def props = Props(new PushReader())

  case class Read(body: Source[ByteString, Any])

  case object CancelStreams
}
