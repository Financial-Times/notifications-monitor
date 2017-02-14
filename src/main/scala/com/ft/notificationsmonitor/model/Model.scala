package com.ft.notificationsmonitor.model

import spray.json.DefaultJsonProtocol._
import spray.json._
import NotificationFormats._

case class PushEntry(apiUrl: String, id: String) extends NotificationEntry(apiUrl, id)

case class PullEntry(apiUrl: String, id: String) extends NotificationEntry(apiUrl, id)

case class PullPage(requestUrl: String, notifications: List[PullEntry])

object NotificationFormats {

  implicit val pushEntryFormat: RootJsonFormat[PushEntry] = DefaultJsonProtocol.jsonFormat(PushEntry.apply, "apiUrl", "id")
  implicit val pullEntryFormat: RootJsonFormat[PullEntry] = DefaultJsonProtocol.jsonFormat(PullEntry.apply, "apiUrl", "id")
  implicit val pullPageFormat: RootJsonFormat[PullPage] = DefaultJsonProtocol.jsonFormat2(PullPage)
}
