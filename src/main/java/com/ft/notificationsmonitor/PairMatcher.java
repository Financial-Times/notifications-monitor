package com.ft.notificationsmonitor;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.NotificationEntry;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PairMatcher extends UntypedActor {

    private static final int INCONSISTENT_INTERVAL_TOLERANCE = 3;

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private String typeA;
    private String typeB;
    private List<DatedEntry> aEntries = new LinkedList<>();
    private List<DatedEntry> bEntries = new LinkedList<>();

    public PairMatcher(String typeA, String typeB) {
        this.typeA = typeA;
        this.typeB = typeB;
    }

    @Override
    public void onReceive(Object message) {
        if (message instanceof DatedEntry) {
            DatedEntry datedEntry = ((DatedEntry) message);
            if (sender().path().name().startsWith(typeA)) {
                matchEntry(datedEntry, aEntries, bEntries, typeA);
            } else if (sender().path().name().startsWith(typeB)) {
                matchEntry(datedEntry, bEntries, aEntries, typeB);
            } else {
                log.warning("What to do with {}", sender().path().name());
            }

        } else if (message.equals("Report")) {
            reportOneSide(aEntries, typeA, typeB);
            reportOneSide(bEntries, typeB, typeA);
        }
    }

    private void matchEntry(final DatedEntry datedEntry, final List<DatedEntry> entries, final List<DatedEntry> oppositeEntries, String notificationType) {
        final NotificationEntry entry = datedEntry.getEntry();
        Optional<DatedEntry> pair = oppositeEntries.stream().filter(p -> p.getEntry().getId().equals(entry.getId())).findFirst();
        if (pair.isPresent()) {
            log.debug("Found pair for {} entry {}", notificationType, entry.getId());
            oppositeEntries.remove(pair.get());
        } else {
            log.debug("Not found pair for {} entry. Adding {}", notificationType, entry.getId());
            entries.add(datedEntry);
        }
    }

    private void reportOneSide(final List<DatedEntry> entries, final String notificationType, final String oppositeNotificationType) {
        List<DatedEntry> toReport = entries.stream().filter(p -> p.getDate().isBefore(ZonedDateTime.now().minusMinutes(INCONSISTENT_INTERVAL_TOLERANCE))).collect(Collectors.toList());
        if (toReport.isEmpty()) {
            log.info("All {} notifications were matched by {} ones. (Not considering the last {} minutes which is tolerated to be inconsistent.)", notificationType, oppositeNotificationType, INCONSISTENT_INTERVAL_TOLERANCE);
        } else {
            toReport.forEach(datedEntry -> {
                log.warning("No pair for {} notification after {} minutes. id={} date={}", notificationType, INCONSISTENT_INTERVAL_TOLERANCE, datedEntry.getEntry().getId(), datedEntry.getDate().format(DateTimeFormatter.ISO_INSTANT));
                entries.remove(datedEntry);
            });
        }
    }

    public static Props props(final String aPrefix, final String bPrefix) {
        return Props.create(new Creator<PairMatcher>() {
            private static final long serialVersionUID = 1L;

            @Override
            public PairMatcher create() throws Exception {
                return new PairMatcher(aPrefix, bPrefix);
            }
        });
    }
}
