package com.ft.notificationsmonitor.http;

import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.OutgoingConnection;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.headers.Authorization;
import akka.http.javadsl.model.headers.RawHeader;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.ft.notificationsmonitor.model.PushHttpConfig;

import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.model.StatusCodes.OK;

public class PushHttp {

    private Materializer mat;

    private final Flow<HttpRequest, HttpResponse, CompletionStage<OutgoingConnection>> connectionFlow;
    private final PushHttpConfig pushHttpConfig;

    public PushHttp(final ActorSystem sys, final PushHttpConfig pushHttpConfig) {
        this.pushHttpConfig = pushHttpConfig;
        mat = ActorMaterializer.create(sys);
        connectionFlow = Http.get(sys).outgoingConnection(ConnectHttp.toHostHttps(pushHttpConfig.getHostname(), pushHttpConfig.getPort()));
    }

    public CompletionStage<Source<ByteString, Object>> makeRequest() {
        final HttpRequest request = HttpRequest.create().withUri(pushHttpConfig.getUri())
                .addHeader(Authorization.basic(pushHttpConfig.getUsername(), pushHttpConfig.getPassword()))
                .addHeader(RawHeader.create("x-api-key", pushHttpConfig.getApiKey()));
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
