package com.ft.notificationsmonitor.model;

import java.time.ZonedDateTime;

public class PullEntry extends NotificationEntry {

    private final ZonedDateTime notificationDate;

    public PullEntry(String id, String publishReference, ZonedDateTime lastModified, ZonedDateTime notificationDate) {
        super(id, publishReference, lastModified);
        this.notificationDate = notificationDate;
    }

    public ZonedDateTime getNotificationDate() {
        return notificationDate;
    }
}
