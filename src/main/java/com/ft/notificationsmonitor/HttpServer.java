package com.ft.notificationsmonitor;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;

import java.util.concurrent.CompletionStage;

public class HttpServer extends AllDirectives {

    private final ActorSystem sys;
    private final ActorMaterializer mat;

    public HttpServer(final ActorSystem sys) {
        this.sys = sys;
        this.mat = ActorMaterializer.create(sys);
    }

    public CompletionStage<ServerBinding> runServer() {
        Route route = get(
                () -> pathSingleSlash( () ->
                        complete("Ok!")
                )
        );

        final Http http = Http.get(sys);
        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = route.flow(sys, mat);
        return http.bindAndHandle(routeFlow, ConnectHttp.toHost("0.0.0.0", 8080), mat);
    }
}
