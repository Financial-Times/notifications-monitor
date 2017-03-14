package com.ft.notificationsmonitor;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.NotificationEntry;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

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

    private void matchEntry(final DatedEntry datedEntry, final List<DatedEntry> entries, final List<DatedEntry> oppositeEntries, final String notificationType) {
        final NotificationEntry entry = datedEntry.getEntry();
        addEntry(datedEntry, entry, entries, notificationType);
        removeMatched(datedEntry, entries, oppositeEntries, notificationType);
    }

    private void removeMatched(final DatedEntry datedEntry, final List<DatedEntry> entries, final List<DatedEntry> oppositeEntries, final String notificationType) {
        final Set<DatedEntry> intersection = oppositeEntries.stream().collect(Collectors.toSet());
        intersection.removeIf(de ->
                entries.stream().noneMatch(ode ->
                        de.getEntry().getId().equals(ode.getEntry().getId()) &&
                                de.getEntry().getPublishReference().equals(ode.getEntry().getPublishReference())
                )
        );
        intersection.forEach(matchedDatedEntry -> {
            entries.removeIf(de ->
                    de.getEntry().getId().equals(matchedDatedEntry.getEntry().getId()) &&
                            de.getEntry().getPublishReference().equals(matchedDatedEntry.getEntry().getPublishReference())
            );
            oppositeEntries.removeIf(de ->
                    de.getEntry().getId().equals(matchedDatedEntry.getEntry().getId()) &&
                            de.getEntry().getPublishReference().equals(matchedDatedEntry.getEntry().getPublishReference())
            );
            log.debug("Matched {} entry id={} publishReference={} matchTimeDiff={}", notificationType, matchedDatedEntry.getEntry().getId(), matchedDatedEntry.getEntry().getPublishReference(), ChronoUnit.MILLIS.between(matchedDatedEntry.getDate(), datedEntry.getDate()));
        });
    }

    private void addEntry(final DatedEntry datedEntry, final NotificationEntry entry, final List<DatedEntry> entries, String notificationType) {
        final Optional<DatedEntry> presentEntryO = entries.stream().filter(p -> p.getEntry().getId().equals(entry.getId())).findFirst();
        if (presentEntryO.isPresent()) {
            resolveDuplicates(datedEntry, entries, presentEntryO.get(), notificationType);
        } else {
            log.debug("Adding {} entry {}", notificationType, entry.getId());
            entries.add(datedEntry);
        }
    }

    private void resolveDuplicates(final DatedEntry datedEntry, final List<DatedEntry> entries, final DatedEntry presentEntry, final String notificationType) {
        final NotificationEntry entry = datedEntry.getEntry();
        if (presentEntry.getEntry().getLastModified().isBefore(entry.getLastModified())) {
            log.info(String.format("Older %s entry with same id was already waiting for a pair. Replacing with newer. uid=%s oldPublishReference=%s oldLastModified=\"%s\" newPublishReference=%s newLastModified=\"%s\"",
                    notificationType, entry.getId(), presentEntry.getEntry().getPublishReference(), presentEntry.getEntry().getLastModified().format(ISO_INSTANT), entry.getPublishReference(), entry.getLastModified().format(ISO_INSTANT)));
            entries.remove(presentEntry);
            entries.add(datedEntry);
        } else {
            log.info(String.format("Newer %s entry with same id was waiting for a pair. Weird situation. Do nothing. uid=%s presentPublishReference=%s presentLastModified=\"%s\" newPublishReference=%s newLastModified=\"%s\"",
                    notificationType, entry.getId(), presentEntry.getEntry().getPublishReference(), presentEntry.getEntry().getLastModified().format(ISO_INSTANT), entry.getPublishReference(), entry.getLastModified().format(ISO_INSTANT)));
        }
    }

    private void reportOneSide(final List<DatedEntry> entries, final String notificationType, final String oppositeNotificationType) {
        List<DatedEntry> toReport = entries.stream().filter(p -> p.getDate().isBefore(ZonedDateTime.now().minusMinutes(INCONSISTENT_INTERVAL_TOLERANCE))).collect(Collectors.toList());
        if (toReport.isEmpty()) {
            log.info("All {} notifications were matched by {} ones. (Not considering the last {} minutes which is tolerated to be inconsistent.)", notificationType, oppositeNotificationType, INCONSISTENT_INTERVAL_TOLERANCE);
        } else {
            toReport.forEach(datedEntry -> {
                log.warning("No pair for {} notification after {} minutes. id={} date={}", notificationType, INCONSISTENT_INTERVAL_TOLERANCE, datedEntry.getEntry().getId(), datedEntry.getDate().format(ISO_INSTANT));
                entries.remove(datedEntry);
            });
        }
    }

    public static Props props(final String typeA, final String typeB) {
        return Props.create(new Creator<PairMatcher>() {
            private static final long serialVersionUID = 1L;

            @Override
            public PairMatcher create() throws Exception {
                return new PairMatcher(typeA, typeB);
            }
        });
    }
}
