package com.ft.notificationsmonitor.model;

public class HttpConfig {

    private String hostname;
    private Integer port;
    private String uri;
    private String username;
    private String password;

    public HttpConfig(String hostname, Integer port, String uri, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.uri = uri;
        this.username = username;
        this.password = password;
    }

    public String getHostname() {
        return hostname;
    }

    public Integer getPort() {
        return port;
    }

    public String getUri() {
        return uri;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
