package com.ft.notificationsmonitor.http;

import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.OutgoingConnection;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.Query;
import akka.http.javadsl.model.headers.Authorization;
import akka.http.javadsl.model.headers.RawHeader;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.ft.notificationsmonitor.model.HttpConfig;
import com.ft.notificationsmonitor.model.NotificationFormats;
import com.ft.notificationsmonitor.model.PullPage;
import spray.json.JsValue;
import spray.json.JsonParser;
import spray.json.ParserInput;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.model.HttpCharsets.UTF_8;
import static akka.http.javadsl.model.StatusCodes.OK;

public class PullHttp {

    private Materializer mat;

    private Flow<HttpRequest, HttpResponse, CompletionStage<OutgoingConnection>> connectionFlow;
    private HttpConfig httpConfig;

    public PullHttp(ActorSystem sys, HttpConfig httpConfig) {
        this.httpConfig = httpConfig;
        mat = ActorMaterializer.create(sys);
        connectionFlow = Http.get(sys).outgoingConnection(ConnectHttp.toHostHttps(httpConfig.getHostname(), httpConfig.getPort()));
    }

    public CompletionStage<PullPage> makeRequest(final Query query, final String tid) {
        HttpRequest request = HttpRequest.GET(httpConfig.getUri() + "?" + query.render(UTF_8))
                .addHeader(Authorization.basic(httpConfig.getUsername(), httpConfig.getPassword()))
                .addHeader(RawHeader.create("X-Request-Id", tid));
        return Source.single(request)
                .via(connectionFlow)
                .runWith(Sink.head(), mat)
                .thenCompose(response -> {
                    if (!response.status().equals(OK)) {
                        return CompletableFuture.supplyAsync(() -> {
                           throw new RuntimeException(String.format("Response status not ok on notifications pull request. status=%d", response.status().intValue()));
                        });
                    } else {
                        return response.entity().toStrict(5000, mat)
                                .thenApply(entity -> entity.getData().utf8String());
                    }
                }).thenApply(responseBody -> {
                    JsValue parser = JsonParser.apply(new ParserInput.StringBasedParserInput(responseBody));
                    return parser.convertTo(NotificationFormats.pullPageFormat());
                });
    }
}
