package com.ft.notificationsmonitor.model;

public class NotificationEntry {

    private String apiUrl;
    private String id;

    public NotificationEntry(String apiUrl, String id) {
        this.apiUrl = apiUrl;
        this.id = id;
    }

    public String getApiUrl() {
        return apiUrl;
    }

    public String getId() {
        return id;
    }
}
