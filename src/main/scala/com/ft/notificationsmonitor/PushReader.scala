package com.ft.notificationsmonitor

import java.time.ZonedDateTime

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.util.ByteString
import com.ft.notificationsmonitor.model.{DatedEntry, PushEntry}
import com.ft.notificationsmonitor.model.NotificationFormats._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class PushReader(private val pairMatcher: ActorRef) extends Actor with ActorLogging {

  implicit private val ec = context.dispatcher
  implicit private val mat = ActorMaterializer()

  private val willCancelStreamP = Promise[Done]()

  override def receive: Receive = {
    case source: Source =>
      val (killSwitch, doneF) = consumeBodyStream(source)

      willCancelStreamP.future.onComplete { _ =>
        killSwitch.shutdown()
        self ! PoisonPill
      }

      doneF.onComplete { _ =>
        log.info("Stream has ended.")
        context.parent ! "StreamEnded"
      }

    case "CancelStreams" =>
      willCancelStreamP.complete(Success(Done))
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
          parseLines(lines)
          ByteString("")
        } else {
          parseLines(lines.dropRight(1))
          ByteString(lines.last)
        }
    }
  }

  private def parseLines(lines: List[String]) = {
    lines.map(_.stripPrefix("data: [").stripSuffix("]")).foreach {
      case "" => log.info("heartbeat")
      case s => parseLine(s)
    }
  }

  private def parseLine(line: String) = {
    Future {
      val jsonLine = line.parseJson
      jsonLine.convertTo[PushEntry]
    }.onComplete {
      case Success(entry) =>
        log.info(entry.id)
        pairMatcher ! DatedEntry(entry, ZonedDateTime.now)
      case Failure(t) => log.error(t, "Error deserializing notifications response")
    }
  }
}

object PushReader {

  def props(pairMatcher: ActorRef) = Props(new PushReader(pairMatcher))

  case class Read(body: Source[ByteString, Any])
}