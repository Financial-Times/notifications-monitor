package com.ft.notificationsmonitor.producer

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.Success

object Producer extends App {

  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val sys = ActorSystem("notificationsmonitor-producer")
  implicit private val mat = ActorMaterializer()
  import sys.dispatcher

  private val willTerminateP = Promise[Done]()

  val route =
    path("content" / "notifications-push") {
      get {
        complete {
          val source = Source.fromIterator[HttpEntity.ChunkStreamPart](() =>
            new EndedRepeat(willTerminateP.future, ByteString("data: [{\"hi\":\"world\"}]\n\n"), sys.dispatcher)
          ).throttle(1, 1 second, 1, ThrottleMode.Shaping)
          HttpResponse(entity = HttpEntity.Chunked(
            ContentTypes.`text/plain(UTF-8)`, source)
          )
        }
      }
    }

  private val httpBindingF = Http().bindAndHandle(route, "localhost", 8080)
  logger.info("Server online at http://localhost:8080/\nPress RETURN to stop...")
  StdIn.readLine()
  willTerminateP.complete(Success(Done))
  Thread.sleep(1100)
  Await.ready(
    Http().shutdownAllConnectionPools()
    .flatMap(_ => httpBindingF)
    .flatMap(_.unbind())
    .flatMap(_ ⇒ sys.terminate()),
  2 seconds)
}

class EndedRepeat[T](private val endF: Future[Done],
                  private val v: T,
                  private val ec: ExecutionContext) extends Iterator[T] {

  implicit private val implicitEc = ec
  private var ended = false

  endF onComplete { _ =>
    ended = true
  }

  override def hasNext: Boolean = !ended

  override def next(): T = {
    v
  }
}
