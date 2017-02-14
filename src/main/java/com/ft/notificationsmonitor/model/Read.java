package com.ft.notificationsmonitor.model;

import akka.stream.javadsl.Source;
import akka.util.ByteString;

public class Read {
    private Source<ByteString, Object> body;

    public Read(Source<ByteString, Object> body) {
        this.body = body;
    }

    public Source<ByteString, Object> getBody() {
        return body;
    }
}
