package com.ft.notificationsmonitor;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.http.javadsl.*;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.headers.Authorization;
import akka.http.javadsl.model.headers.BasicHttpCredentials;
import akka.japi.Creator;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.HttpConfig;
import com.ft.notificationsmonitor.model.NotificationFormats;
import scala.collection.JavaConverters;
import spray.json.JsValue;
import spray.json.JsonParser;
import spray.json.ParserInput;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.model.StatusCodes.OK;

public class PullConnector extends UntypedActor {

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private Materializer mat = ActorMaterializer.create(context());

    private Flow<HttpRequest, HttpResponse, CompletionStage<OutgoingConnection>> connectionFlow;
    private HttpConfig httpConfig;
    private ActorRef pairMatcher;
    private ZonedDateTime last = ZonedDateTime.now();

    public PullConnector (HttpConfig httpConfig, ActorRef pairMatcher) {
        this.httpConfig = httpConfig;
        this.pairMatcher = pairMatcher;
        connectionFlow = Http.get(context().system()).outgoingConnection(ConnectHttp.toHostHttps(httpConfig.hostname(), httpConfig.port()));
    }

    @Override
    public void onReceive(Object message) {
        if (message.equals("RequestSinceLast")) {
            makeRequest(last);
        }
    }

    private void makeRequest(ZonedDateTime date) {
        HttpRequest request = HttpRequest.create(httpConfig.uri() + "?since=" + date.format(DateTimeFormatter.ISO_INSTANT))
                .addHeader(Authorization.create(BasicHttpCredentials.create(httpConfig.credentials()._1(), httpConfig.credentials()._2())));
        final CompletionStage<HttpResponse> responseF = Source.single(request)
                .via(connectionFlow)
                .runWith(Sink.head(), mat);
        responseF.whenComplete((response, failure) -> {
            if (failure != null) {
                log.error(failure, "Failed request. host={} uri={}", httpConfig.hostname(), httpConfig.uri());
            } else {
                if (!response.status().equals(OK)) {
                    log.warning("Response status not ok. Retrying in a few moments... host={} uri={} status={}", httpConfig.hostname(), httpConfig.uri(), response.status().intValue());
                } else {
                    last = ZonedDateTime.now();
                    response.entity().toStrict(5000, mat)
                            .thenAccept(httpEntity -> parsePage(httpEntity.getData().utf8String()));
                }
            }
        });
    }

    private void parsePage(final String pageText) {
        CompletableFuture.supplyAsync(() -> {
            JsValue parser = JsonParser.apply(new ParserInput.StringBasedParserInput(pageText));
            return parser.convertTo(NotificationFormats.pullPageFormat());
        }).whenComplete((page, failure) -> {
            if (failure != null) {
                log.error(failure, "Error deserializing notifications response: {}", pageText);
            } else {
                JavaConverters.asJavaCollection(page.notifications()).forEach((entry) -> {
                    log.info(entry.id());
                    pairMatcher.tell(new DatedEntry(entry, ZonedDateTime.now()), self());
                });
            }
        });
    }

    public static Props props(final HttpConfig httpConfig, final ActorRef pairMatcher) {
        return Props.create(new Creator<PullConnector>() {
            private static final long serialVersionUID = 1L;

            @Override
            public PullConnector create() throws Exception {
                return new PullConnector(httpConfig, pairMatcher);
            }
        });
    }
}
