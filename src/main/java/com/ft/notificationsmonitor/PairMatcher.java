package com.ft.notificationsmonitor;

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

    private static final int INCONSISTENT_INTERVAL_TOLERANCE = 3;

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private List<DatedEntry> pushEntries = new LinkedList<>();
    private List<DatedEntry> pullEntries = new LinkedList<>();

    @Override
    public void onReceive(Object message) {
        if (message instanceof DatedEntry) {
            DatedEntry datedEntry = ((DatedEntry) message);
            NotificationEntry entry = datedEntry.entry();
            if (entry instanceof PushEntry) {
                matchEntry(datedEntry, pushEntries, pullEntries, "push");
            } else if (entry instanceof PullEntry) {
                matchEntry(datedEntry, pullEntries, pushEntries, "pull");
            }

        } else if (message.equals("Report")) {
            log.info("Reporting.");
            reportOneSide(pushEntries, "push");
            reportOneSide(pullEntries, "pull");
            log.info("Report finished.");
        }
    }

    private void matchEntry(final DatedEntry datedEntry, final List<DatedEntry> entries, final List<DatedEntry> oppositeEntries, String notificationType) {
        final NotificationEntry entry = datedEntry.entry();
        Optional<DatedEntry> pair = oppositeEntries.stream().filter(p -> p.entry().id().equals(entry.id())).findFirst();
        if (pair.isPresent()) {
            log.debug("Found pair for {} entry {}", notificationType, entry.id());
            oppositeEntries.remove(pair.get());
        } else {
            log.debug("Not found pair for {} entry. Adding {}", notificationType, entry.id());
            entries.add(datedEntry);
        }
    }

    private void reportOneSide(final List<DatedEntry> entries, final String notificationType) {
        List<DatedEntry> toReport = entries.stream().filter(p -> p.date().isBefore(ZonedDateTime.now().minusMinutes(2))).collect(Collectors.toList());
        if (toReport.isEmpty()) {
            log.info("All {} notifications were matched by pull ones. (Not considering the last {} minutes which is tolerated to be inconsistent.)", notificationType, INCONSISTENT_INTERVAL_TOLERANCE);
        } else {
            toReport.forEach(datedEntry -> {
                log.warning("No pair for {} notification after {} minutes. id={} date={}", notificationType, INCONSISTENT_INTERVAL_TOLERANCE, datedEntry.entry().id(), datedEntry.date().format(DateTimeFormatter.ISO_INSTANT));
                entries.remove(datedEntry);
            });
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
