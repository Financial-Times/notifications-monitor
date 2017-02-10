package com.ft.notificationsmonitor.model

import java.time.ZonedDateTime

import spray.json.DefaultJsonProtocol._
import spray.json._
import NotificationFormats._

abstract class NotificationEntry(val apiUrl: String, val id: String) {}

case class DatedEntry(entry: NotificationEntry, date: ZonedDateTime)

case class PushEntry(override val apiUrl: String, override val id: String) extends NotificationEntry(apiUrl, id)

case class PullEntry(override val apiUrl: String, override val id: String) extends NotificationEntry(apiUrl, id)

case class PullPage(requestUrl: String, notifications: List[PullEntry], links: List[Link])

case class Link(href: String, rel: String)

case class HttpConfig(hostname: String, port: Int, uri: String, credentials: (String, String))

object NotificationFormats {

  implicit val pushEntryFormat: RootJsonFormat[PushEntry] = DefaultJsonProtocol.jsonFormat(PushEntry.apply, "apiUrl", "id")
  implicit val pullEntryFormat: RootJsonFormat[PullEntry] = DefaultJsonProtocol.jsonFormat(PullEntry.apply, "apiUrl", "id")
  implicit val linkFormat: RootJsonFormat[Link] = DefaultJsonProtocol.jsonFormat2(Link)
  implicit val pullPageFormat: RootJsonFormat[PullPage] = DefaultJsonProtocol.jsonFormat3(PullPage)
}
