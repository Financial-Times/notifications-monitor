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
}
