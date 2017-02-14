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
import com.ft.notificationsmonitor.model.HttpConfig;
import scala.concurrent.duration.Duration;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static akka.http.javadsl.model.StatusCodes.OK;

public class PushConnector extends UntypedActor {

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private Materializer mat = ActorMaterializer.create(context());
    private HttpConfig httpConfig;
    private ActorRef pairMatcher;
    private ActorRef reader;
    private Flow<HttpRequest, HttpResponse, CompletionStage<OutgoingConnection>> connectionFlow;
    private boolean cancelStreams = false;

    public PushConnector(HttpConfig httpConfig, ActorRef pairMatcher) {
        this.httpConfig = httpConfig;
        this.pairMatcher = pairMatcher;
        reader = context().actorOf(PushReader.props(pairMatcher));
        connectionFlow = Http.get(context().system()).outgoingConnection(ConnectHttp.toHostHttps(httpConfig.hostname(), httpConfig.port()));
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message.equals("Connect")) {
            HttpRequest request = HttpRequest.create(httpConfig.uri())
                    .addHeader(Authorization.basic(httpConfig.credentials()._1(), httpConfig.credentials()._2()));
            Source.single(request)
                    .via(connectionFlow)
                    .runWith(Sink.head(), mat)
                    .whenComplete((response, failure) -> {
                        if (failure != null) {
                            log.error("Failed request. Retrying in a few moments... host={} uri={}", httpConfig.hostname(), httpConfig.uri(), failure);
                            context().system().scheduler().scheduleOnce(Duration.apply(5, TimeUnit.SECONDS), self(), "Connect", context().dispatcher(), self());
                        } else {
                            if (!response.status().equals(OK)) {
                                log.warning("Response status not ok. Retrying in a few moments... host={} uri={} status={}", httpConfig.hostname(), httpConfig.uri(), response.status().intValue());
                                context().system().scheduler().scheduleOnce(Duration.apply(5, TimeUnit.SECONDS), self(), "Connect", context().dispatcher(), self());
                            } else {
                                log.info("Connected to push feed. host={} uri={} status={}", httpConfig.hostname(), httpConfig.uri(), response.status().intValue());
                                reader = context().actorOf(PushReader.props(pairMatcher));
                                reader.tell(response.entity().getDataBytes(), self());
                            }
                        }
                    });
        } else if (message.equals("CancelStreams")) {
            cancelStreams = true;
            reader.tell("CancelStreams", self());
        } else if (message.equals("StreamEnded")) {
            if (!cancelStreams) {
                self().tell("Connect", self());
            }
        }
    }

    public static Props props(final HttpConfig httpConfig, final ActorRef pairMatcher) {
        return Props.create(new Creator<PushConnector>() {
            private static final long serialVersionUID = 1L;

            @Override
            public PushConnector create() throws Exception {
                return new PushConnector(httpConfig, pairMatcher);
            }
        });
    }
}
