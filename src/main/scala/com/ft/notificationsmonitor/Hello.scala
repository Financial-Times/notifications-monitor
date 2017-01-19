package com.ft.notificationsmonitor

import java.io.File

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, KillSwitches}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}

object Hello extends App {

  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val sys = ActorSystem("notifications-monitor")
  implicit private val mat = ActorMaterializer(ActorMaterializerSettings(sys).withDebugLogging(true))

  import sys.dispatcher

  private val sensitiveConfig = ConfigFactory.parseFile(new File("src/main/resources/.sensitive.conf"))
  ConfigFactory.load().withFallback(sensitiveConfig)

  scala.sys addShutdownHook shutdown

  private val killPromise = Promise[Done]()

  val connectionFlow = Http().outgoingConnectionHttps("pre-prod-uk-up.ft.com")
  val request = HttpRequest(uri = "/content/notifications-push")
    .addHeader(Authorization(BasicHttpCredentials(
      sensitiveConfig.getString("basic-auth.username"),
      sensitiveConfig.getString("basic-auth.password")
    )))
  val responseF = Source.single(request)
    .via(connectionFlow)
    .runWith(Sink.head)
  responseF onComplete {
    case Failure(exception) => logger.warn("Failed request", exception)
    case Success(response) =>
      logger.info(response.status.value)
      val killSwitch = consumeBodyByPrinting(response.entity.dataBytes)
      killPromise.future.onComplete(_ => killSwitch.shutdown())
  }

  private def consumeBodyByPrinting(dataBytes: Source[ByteString, Any]) = {
    dataBytes
      .viaMat(KillSwitches.single)(Keep.right)
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

  private def shutdown() = {
    logger.info("Exiting...")
    killPromise.complete(Success(Done))
    Await.ready(
      Http().shutdownAllConnectionPools()
        .flatMap(_ => sys.terminate()), 5 seconds
    )
  }
}
