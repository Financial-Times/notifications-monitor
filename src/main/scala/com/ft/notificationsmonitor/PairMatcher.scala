package com.ft.notificationsmonitor

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import akka.actor.{Actor, ActorLogging, Props}
import PairMatcher._
import com.ft.notificationsmonitor.model._

import scala.collection.mutable

class PairMatcher extends Actor with ActorLogging {

  private val InconsistentIntervalTolerance = 3

  private val pushEntries = mutable.ArrayBuffer[DatedEntry]()
  private val pullEntries = mutable.ArrayBuffer[DatedEntry]()

  override def receive: Receive = {
    case datedEntry: DatedEntry =>
      datedEntry.entry match {
        case _: PushEntry =>
          matchEntry(datedEntry, pushEntries, pullEntries, "push")
        case _: PullEntry =>
          matchEntry(datedEntry, pullEntries, pushEntries, "pull")
      }

    case Report =>
      reportOneSide(pushEntries, "push", "pull")
      reportOneSide(pullEntries, "pull", "push")
  }

  private def matchEntry(datedEntry: DatedEntry, entries: mutable.ArrayBuffer[DatedEntry], oppositeEntries: mutable.ArrayBuffer[DatedEntry], notificationType: String) {
    oppositeEntries.find(p => p.entry.id.equals(datedEntry.entry.id)) match {
      case None =>
        log.debug("Not found pair for {} entry. Adding {}", notificationType, datedEntry.entry.id)
        entries.append(datedEntry)
      case Some(matchedEntry) =>
        log.debug("Found pair for {} entry {}", notificationType, datedEntry.entry.id)
        oppositeEntries.remove(oppositeEntries.indexOf(matchedEntry))
    }
  }

  private def reportOneSide(entries: mutable.ArrayBuffer[DatedEntry], notificationType: String, oppositeNotificationType: String) {
    val toReport = entries.filter(p => p.date.isBefore(ZonedDateTime.now().minusMinutes(InconsistentIntervalTolerance)))
    if (toReport.isEmpty) {
      log.info("All {} notifications were matched by {} ones. (Not considering the last {} minutes which is tolerated to be inconsistent.)", notificationType, oppositeNotificationType, InconsistentIntervalTolerance)
    } else {
      toReport.foreach { datedEntry =>
        log.warning("No pair for {} notification after {} minutes. id={} date={}", notificationType, InconsistentIntervalTolerance, datedEntry.entry.id, datedEntry.date.format(DateTimeFormatter.ISO_INSTANT))
        entries.remove(entries.indexOf(datedEntry))
      }
    }
  }
}

object PairMatcher {

  def props = Props(new PairMatcher())

  case object Report
}
