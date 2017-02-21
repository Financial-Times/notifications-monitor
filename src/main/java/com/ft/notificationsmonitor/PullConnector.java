package com.ft.notificationsmonitor;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.OutgoingConnection;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.headers.Authorization;
import akka.japi.Creator;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.HttpConfig;
import com.ft.notificationsmonitor.model.NotificationFormats;
import com.ft.notificationsmonitor.model.PullEntry;
import scala.collection.JavaConverters;
import spray.json.JsValue;
import spray.json.JsonParser;
import spray.json.ParserInput;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.model.StatusCodes.OK;

public class PullConnector extends UntypedActor {

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private Materializer mat = ActorMaterializer.create(getContext());

    private Flow<HttpRequest, HttpResponse, CompletionStage<OutgoingConnection>> connectionFlow;
    private HttpConfig httpConfig;
    private List<ActorRef> pairMatchers;
    private ZonedDateTime last = ZonedDateTime.now();

    public PullConnector (HttpConfig httpConfig, List<ActorRef> pairMatchers) {
        this.httpConfig = httpConfig;
        this.pairMatchers = pairMatchers;
        connectionFlow = Http.get(context().system()).outgoingConnection(ConnectHttp.toHostHttps(httpConfig.getHostname(), httpConfig.getPort()));
    }

    @Override
    public void onReceive(Object message) {
        if (message.equals("RequestSinceLast")) {
            makeRequest(last);
        }
    }

    private void makeRequest(ZonedDateTime date) {
        HttpRequest request = HttpRequest.create(httpConfig.getUri() + "?since=" + date.format(DateTimeFormatter.ISO_INSTANT))
                .addHeader(Authorization.basic(httpConfig.getUsername(), httpConfig.getPassword()));
        final CompletionStage<HttpResponse> responseF = Source.single(request)
                .via(connectionFlow)
                .runWith(Sink.head(), mat);
        responseF.whenComplete((response, failure) -> {
            if (failure != null) {
                log.error(failure, "Failed request. host={} uri={}", httpConfig.getHostname(), httpConfig.getUri());
            } else {
                if (!response.status().equals(OK)) {
                    log.warning("Response status not ok. Retrying in a few moments... host={} uri={} status={}", httpConfig.getHostname(), httpConfig.getUri(), response.status().intValue());
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
                final Collection<PullEntry> notifications = JavaConverters.asJavaCollection(page.notifications());
                if (notifications.isEmpty()) {
                    log.info("heartbeat");
                } else {
                    notifications.forEach(entry -> {
                        log.info(entry.id());
                        final DatedEntry datedEntry = new DatedEntry(entry, ZonedDateTime.now());
                        pairMatchers.forEach(pairMatcher -> pairMatcher.tell(datedEntry, self()));
                    });
                }
            }
        });
    }

    public static Props props(final HttpConfig httpConfig, final List<ActorRef> pairMatchers) {
        return Props.create(new Creator<PullConnector>() {
            private static final long serialVersionUID = 1L;

            @Override
            public PullConnector create() throws Exception {
                return new PullConnector(httpConfig, pairMatchers);
            }
        });
    }
}
