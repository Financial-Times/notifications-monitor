package com.ft.notificationsmonitor.model;

import akka.http.javadsl.model.Query;
import akka.http.javadsl.model.Uri;
import akka.japi.Pair;

public class PushHttpConfig extends HttpConfig {

    private String apiKey;

    public PushHttpConfig(String hostname, Integer port, String uri, String username, String password, String apiKey) {
        super(hostname, port, uri, username, password);
        this.apiKey = apiKey;
    }

    public String getApiKey() {
        return apiKey;
    }

    @Override
    public Uri getUri() {
        return super.getUri().query(Query.create(new Pair<>("monitor", "true")));
    }
}
