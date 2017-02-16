package com.ft.notificationsmonitor.model

import spray.json.DefaultJsonProtocol._
import spray.json._
import NotificationFormats._

case class PushEntry(id: String) extends NotificationEntry(id)

case class PullEntry(id: String) extends NotificationEntry(id)

case class PullPage(notifications: List[PullEntry])

object NotificationFormats {

  implicit val pushEntryFormat: RootJsonFormat[PushEntry] = DefaultJsonProtocol.jsonFormat(PushEntry.apply, "id")
  implicit val pullEntryFormat: RootJsonFormat[PullEntry] = DefaultJsonProtocol.jsonFormat(PullEntry.apply, "id")
  implicit val pullPageFormat: RootJsonFormat[PullPage] = DefaultJsonProtocol.jsonFormat1(PullPage)
}
