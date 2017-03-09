package com.ft.notificationsmonitor;

import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.NotificationEntry;

import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

public class History {

    private Queue<DatedEntry> history = new LinkedList<>();

    /**
     * return false if found in history. true otherwise.
     */
    public boolean verifyAndAddToHistory(final DatedEntry datedEntry) {
        final NotificationEntry entry = datedEntry.getEntry();
        final Optional<DatedEntry> presentEntryO = history.stream().filter(e -> e.getEntry().getId().equals(entry.getId()) &&
                e.getEntry().getPublishReference().equals(entry.getPublishReference())).findFirst();
        history.add(datedEntry);
        return !presentEntryO.isPresent();
    }

    public void clearSomeHistory() {
        history = new LinkedList<>(
                history.stream()
                        .filter(e -> e.getDate().isBefore(ZonedDateTime.now().minusMinutes(5)))
                        .collect(Collectors.toList())
        );
    }
}
