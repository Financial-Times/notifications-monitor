package com.ft.notificationsmonitor.model;

import akka.http.javadsl.model.Uri;

public class HttpConfig {

    private String hostname;
    private Integer port;
    private Uri uri;
    private String username;
    private String password;

    public HttpConfig(String hostname, Integer port, String uri, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.uri = Uri.create(uri);
        this.username = username;
        this.password = password;
    }

    public String getHostname() {
        return hostname;
    }

    public Integer getPort() {
        return port;
    }

    public Uri getUri() {
        return uri;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
