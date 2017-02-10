package com.ft.notificationsmonitor.model

import spray.json.DefaultJsonProtocol._
import spray.json._
import PullEntryFormat._
import LinkFormat._

case class PushEntry(apiUrl: String, id: String)

case class PullEntry(apiUrl: String, id: String)

object PushEntryFormat {

  implicit val pushEntryFormat: RootJsonFormat[PushEntry] = DefaultJsonProtocol.jsonFormat2(PushEntry)

}

object PullEntryFormat {

  implicit val pullEntryFormat: RootJsonFormat[PullEntry] = DefaultJsonProtocol.jsonFormat2(PullEntry)
}

case class PullPage(requestUrl: String, notifications: List[PullEntry], links: List[Link])

case class Link(href: String, rel: String)

object PullPageFormat {

  implicit val pullPageFormat: RootJsonFormat[PullPage] = DefaultJsonProtocol.jsonFormat3(PullPage)

}

object LinkFormat {

  implicit val linkFormat: RootJsonFormat[Link] = DefaultJsonProtocol.jsonFormat2(Link)
}

case class HttpConfig(hostname: String, port: Int, uri: String, credentials: (String, String))