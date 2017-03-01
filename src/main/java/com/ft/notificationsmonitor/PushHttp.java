package com.ft.notificationsmonitor;

import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.OutgoingConnection;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.headers.Authorization;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.ft.notificationsmonitor.model.HttpConfig;

import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.model.StatusCodes.OK;

public class PushHttp {

    private Materializer mat;

    private final Flow<HttpRequest, HttpResponse, CompletionStage<OutgoingConnection>> connectionFlow;
    private final HttpConfig httpConfig;

    public PushHttp(final ActorSystem sys, final HttpConfig httpConfig) {
        this.httpConfig = httpConfig;
        mat = ActorMaterializer.create(sys);
        connectionFlow = Http.get(sys).outgoingConnection(ConnectHttp.toHostHttps(httpConfig.getHostname(), httpConfig.getPort()));
    }

    public CompletionStage<Source<ByteString, Object>> makeRequest() {
        final HttpRequest request = HttpRequest.GET(httpConfig.getUri())
                .addHeader(Authorization.basic(httpConfig.getUsername(), httpConfig.getPassword()));
        return Source.single(request)
                .via(connectionFlow)
                .runWith(Sink.head(), mat)
                .thenApply(response -> {
                        if (!response.status().equals(OK)) {
                            throw new RuntimeException(String.format("Response status not ok on notifications-push request. status=%d", response.status().intValue()));
                        } else {
                            return response.entity().getDataBytes();
                        }
                });
    }
}
