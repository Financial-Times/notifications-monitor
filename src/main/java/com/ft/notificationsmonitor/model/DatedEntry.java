package com.ft.notificationsmonitor.model;

import java.time.ZonedDateTime;

public class DatedEntry {

    private NotificationEntry entry;
    private ZonedDateTime date;

    public DatedEntry(NotificationEntry entry, ZonedDateTime date) {
        this.entry = entry;
        this.date = date;
    }

    public NotificationEntry getEntry() {
        return entry;
    }

    public ZonedDateTime getDate() {
        return date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatedEntry that = (DatedEntry) o;
        return entry.equals(that.entry);
    }

    @Override
    public int hashCode() {
        return entry.hashCode();
    }
}
