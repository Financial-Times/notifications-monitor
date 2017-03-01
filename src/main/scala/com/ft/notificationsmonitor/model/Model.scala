package com.ft.notificationsmonitor.model

import java.time.ZonedDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import spray.json.DefaultJsonProtocol._
import spray.json._
import NotificationFormats._

case class PushEntry(id: String, publishReference: String, lastModified: ZonedDateTime)
  extends NotificationEntry(id, publishReference, lastModified)

case class PullEntry(id: String, publishReference: String, lastModified: ZonedDateTime)
  extends NotificationEntry(id, publishReference, lastModified)

case class Link(href: String)

case class PullPage(notifications: List[PullEntry], links: List[Link])

object NotificationFormats {

  implicit val zonedDateTimeFormat = ZonedDateTimeFormat
  implicit val pushEntryFormat: RootJsonFormat[PushEntry] = DefaultJsonProtocol.jsonFormat3(PushEntry)
  implicit val pullEntryFormat: RootJsonFormat[PullEntry] = DefaultJsonProtocol.jsonFormat3(PullEntry)
  implicit val linkFormat: RootJsonFormat[Link] = DefaultJsonProtocol.jsonFormat1(Link)
  implicit val pullPageFormat: RootJsonFormat[PullPage] = DefaultJsonProtocol.jsonFormat2(PullPage)
}

object ZonedDateTimeFormat extends JsonFormat[ZonedDateTime] {

  def write(date: ZonedDateTime): JsValue = JsString(date.format(DateTimeFormatter.ISO_INSTANT))

  def read(json: JsValue): ZonedDateTime = json match {
    case JsString(rawDate) =>
      try {
        ZonedDateTime.parse(rawDate)
      } catch  {
        case ex: DateTimeParseException =>
          deserializationError(s"Expected ISO Date format, got $rawDate $ex")
      }
    case unknown => deserializationError(s"Expected JsString, got $unknown")
  }
}