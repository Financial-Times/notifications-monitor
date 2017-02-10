package com.ft.notificationsmonitor

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import akka.actor.{Actor, ActorLogging, Props}
import PairMatcher._
import com.ft.notificationsmonitor.model._

import scala.collection.mutable

class PairMatcher extends Actor with ActorLogging {

  private val pushEntries = mutable.ArrayBuffer[DatedEntry]()
  private val pullEntries = mutable.ArrayBuffer[DatedEntry]()

  override def receive: Receive = {

    case DatedEntry(pushEntry: PushEntry, date: ZonedDateTime) =>
      pullEntries.find(p => p.entry.id.equals(pushEntry.id)) match {
        case Some(pair) =>
          log.debug("Found pair for push entry {}", pushEntry.id)
          pullEntries.remove(pullEntries.indexOf(pair))
        case None =>
          log.debug("Not found pair for push entry. Adding {}", pushEntry.id)
          pushEntries.append(DatedEntry(pushEntry, date))
      }

    case DatedEntry(pullEntry: PullEntry, date: ZonedDateTime) =>
      pushEntries.find(p => p.entry.id.equals(pullEntry.id)) match {
        case Some(pair) =>
          log.debug("Found pair for pull entry {}", pullEntry.id)
          pushEntries.remove(pushEntries.indexOf(pair))
        case None =>
          log.debug("Not found pair for pull entry. Adding {}", pullEntry.id)
          pullEntries.append(DatedEntry(pullEntry, date))
      }

    case Report =>
      val pushToReport = pushEntries.filter(p => p.date.isBefore(ZonedDateTime.now.minusMinutes(2)))
      if (pushToReport.isEmpty) {
        log.info("All push notifications were matched by pull ones. (Not considering the last two minutes which is tolerated to be inconsistent.)")
      } else {
        pushToReport.foreach{ datedEntry =>
          log.warning("No pair for push notification after 2 minutes. id={} date={}", datedEntry.entry.id, datedEntry.date.format(DateTimeFormatter.ISO_INSTANT))
        }
      }

      val pullToReport = pullEntries.filter(p => p.date.isBefore(ZonedDateTime.now.minusMinutes(2)))
      if (pullToReport.isEmpty) {
        log.info("All pull notifications were matched by push ones. (Not considering the last two minutes which is tolerated to be inconsistent.)")
      } else {
        pullToReport.foreach{ datedEntry =>
          log.warning("No pair for (pull) notification after 2 minutes. id={} date={}", datedEntry.entry.id, datedEntry.date.format(DateTimeFormatter.ISO_INSTANT))
        }
      }
      log.info("Report finished.")
  }
}

object PairMatcher {

  def props = Props(new PairMatcher())

  case object Report
}
