package com.ft.notificationsmonitor.model;

import java.time.ZonedDateTime;

public class NotificationEntry {

    private String id;
    private String publishReference;
    private ZonedDateTime lastModified;

    public NotificationEntry(String id, String publishReference, ZonedDateTime lastModified) {
        this.id = id;
        this.publishReference = publishReference;
        this.lastModified = lastModified;
    }

    public String getId() {
        return id;
    }

    public String getPublishReference() {
        return publishReference;
    }

    public ZonedDateTime getLastModified() {
        return lastModified;
    }
}
