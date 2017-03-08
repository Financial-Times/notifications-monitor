package com.ft.notificationsmonitor.http;

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
import com.ft.notificationsmonitor.model.HttpConfig;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.model.StatusCodes.NOT_FOUND;
import static akka.http.javadsl.model.StatusCodes.OK;

public class NativeHttp {

    private Materializer mat;

    private Flow<HttpRequest, HttpResponse, CompletionStage<OutgoingConnection>> connectionFlow;
    private HttpConfig httpConfig;

    public NativeHttp(ActorSystem sys, HttpConfig httpConfig) {
        this.httpConfig = httpConfig;
        mat = ActorMaterializer.create(sys);
        connectionFlow = Http.get(sys).outgoingConnection(ConnectHttp.toHostHttps(httpConfig.getHostname(), httpConfig.getPort()));
    }

    public CompletionStage<Optional<String>> getNativeContent(final String id) {
        final String uuid = id.substring(id.lastIndexOf("/") + 1, id.length());
        final HttpRequest request = HttpRequest.GET(httpConfig.getUri() + uuid)
                .addHeader(Authorization.basic(httpConfig.getUsername(), httpConfig.getPassword()));
        return Source.single(request)
                .via(connectionFlow)
                .runWith(Sink.head(), mat)
                .thenCompose(response -> {
                    if (response.status().equals(OK)) {
                        return response.entity().toStrict(5000, mat)
                                .thenApply(entity -> Optional.of(entity.getData().utf8String()));
                    } else if (response.status().equals(NOT_FOUND)){
                        return CompletableFuture.completedFuture(Optional.empty());
                    } else {
                        throw new RuntimeException(String.format("Getting native content resulted with unexpected status=%d", response.status().intValue()));
                    }
                });
    }
}
