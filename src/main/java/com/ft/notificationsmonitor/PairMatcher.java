package com.ft.notificationsmonitor;

import akka.actor.Actor;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.NotificationEntry;
import com.ft.notificationsmonitor.model.PullEntry;
import com.ft.notificationsmonitor.model.PushEntry;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PairMatcher extends UntypedActor {

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private List<DatedEntry> pushEntries = new LinkedList<>();
    private List<DatedEntry> pullEntries = new LinkedList<>();

    @Override
    public void onReceive(Object message) {
        if (message instanceof DatedEntry) {
            DatedEntry datedEntry = ((DatedEntry) message);
            NotificationEntry entry = datedEntry.entry();
            if (entry instanceof PushEntry) {
                PushEntry pushEntry = (PushEntry) entry;
                Optional<DatedEntry> pair = pullEntries.stream().filter(p -> p.entry().id().equals(pushEntry.id())).findFirst();
                if (pair.isPresent()) {
                    log.debug("Found pair for push entry {}", pushEntry.id());
                    pullEntries.remove(pullEntries.indexOf(pair.get()));
                } else {
                    log.debug("Not found pair for push entry. Adding {}", pushEntry.id());
                    pushEntries.add(new DatedEntry(pushEntry, datedEntry.date()));
                }

            } else if (entry instanceof PullEntry) {
                PullEntry pullEntry = (PullEntry) entry;
                Optional<DatedEntry> pair = pushEntries.stream().filter(p -> p.entry().id().equals(pullEntry.id())).findFirst();
                if (pair.isPresent()) {
                    log.debug("Found pair for pull entry {}", pullEntry.id());
                    pushEntries.remove(pullEntries.indexOf(pair.get()));
                } else {
                    log.debug("Not found pair for pull entry. Adding {}", pullEntry.id());
                    pullEntries.add(new DatedEntry(pullEntry, datedEntry.date()));
                }
            }

        } else if (message.equals("Report")) {
            List<DatedEntry> pushToReport = pushEntries.stream().filter(p -> p.date().isBefore(ZonedDateTime.now().minusMinutes(2))).collect(Collectors.toList());
            if (pushToReport.isEmpty()) {
                log.info("All push notifications were matched by pull ones. (Not considering the last two minutes which is tolerated to be inconsistent.)");
            } else {
                pushToReport.forEach((datedEntry) -> log.warning("No pair for push notification after 2 minutes. id={} date={}", datedEntry.entry().id(), datedEntry.date().format(DateTimeFormatter.ISO_INSTANT)));
            }

            List<DatedEntry>  pullToReport = pullEntries.stream().filter(p -> p.date().isBefore(ZonedDateTime.now().minusMinutes(2))).collect(Collectors.toList());
            if (pullToReport.isEmpty()) {
                log.info("All pull notifications were matched by push ones. (Not considering the last two minutes which is tolerated to be inconsistent.)");
            } else {
                pullToReport.forEach((datedEntry) -> log.warning("No pair for (pull) notification after 2 minutes. id={} date={}", datedEntry.entry().id(), datedEntry.date().format(DateTimeFormatter.ISO_INSTANT)));
            }
            log.info("Report finished.");
        }
    }

    public static Props props() {
        return Props.create(new Creator<PairMatcher>() {
            private static final long serialVersionUID = 1L;

            @Override
            public PairMatcher create() throws Exception {
                return new PairMatcher();
            }
        });
    }
}
