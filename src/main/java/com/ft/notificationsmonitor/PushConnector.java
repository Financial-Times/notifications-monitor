package com.ft.notificationsmonitor;

import akka.actor.*;
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
import com.ft.notificationsmonitor.model.Read;
import scala.concurrent.duration.Duration;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static akka.http.javadsl.model.StatusCodes.OK;

public class PushConnector extends UntypedActor {

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private Materializer mat = ActorMaterializer.create(getContext());
    private HttpConfig httpConfig;
    private ActorRef pairMatcher;
    private ActorRef reader;
    private Flow<HttpRequest, HttpResponse, CompletionStage<OutgoingConnection>> connectionFlow;
    private Cancellable heartbeatMonitor;
    private boolean cancelAllStreams = false;

    public PushConnector(HttpConfig httpConfig, ActorRef pairMatcher) {
        this.httpConfig = httpConfig;
        this.pairMatcher = pairMatcher;
        connectionFlow = Http.get(context().system()).outgoingConnection(ConnectHttp.toHostHttps(httpConfig.getHostname(), httpConfig.getPort()));
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message.equals("Connect")) {
            HttpRequest request = HttpRequest.create(httpConfig.getUri())
                    .addHeader(Authorization.basic(httpConfig.getUsername(), httpConfig.getPassword()));
            Source.single(request)
                    .via(connectionFlow)
                    .runWith(Sink.head(), mat)
                    .whenComplete((response, failure) -> {
                        if (failure != null) {
                            log.error("Failed request. Retrying in a few moments... host={} uri={}", httpConfig.getHostname(), httpConfig.getUri(), failure);
                            getContext().system().scheduler().scheduleOnce(Duration.apply(5, TimeUnit.SECONDS), self(), "Connect", getContext().dispatcher(), self());
                        } else {
                            if (!response.status().equals(OK)) {
                                log.warning("Response status not ok. Retrying in a few moments... host={} uri={} status={}", httpConfig.getHostname(), httpConfig.getUri(), response.status().intValue());
                                getContext().system().scheduler().scheduleOnce(Duration.apply(5, TimeUnit.SECONDS), self(), "Connect", getContext().dispatcher(), self());
                            } else {
                                log.info("Connected to push feed. host={} uri={} status={}", httpConfig.getHostname(), httpConfig.getUri(), response.status().intValue());
                                reader = getContext().actorOf(PushReader.props(pairMatcher), "pushReader");
                                getContext().watch(reader);
                                reader.tell(new Read(response.entity().getDataBytes()), self());
                                heartbeatMonitor = getContext().system().scheduler().schedule(Duration.apply(1, TimeUnit.MINUTES) ,
                                        Duration.apply(1, TimeUnit.MINUTES), reader, "CheckHeartbeat", getContext().dispatcher(), ActorRef.noSender());
                            }
                        }
                    });
        } else if (message.equals("CancelAllStreams")) {
            cancelAllStreams = true;
            reader.tell("CancelStream", self());
        } else if (message.equals("StreamEnded")) {
            reader.tell(PoisonPill$.MODULE$, self());
            heartbeatMonitor.cancel();
            if (!cancelAllStreams) {
                self().tell("Connect", self());
            }
        } else if (message instanceof Terminated) {
            log.warning("{} dead.", ((Terminated) message).actor().path());
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