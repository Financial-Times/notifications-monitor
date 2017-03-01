package com.ft.notificationsmonitor;

import akka.NotUsed;
import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import com.ft.notificationsmonitor.http.PushHttp;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import static com.ft.notificationsmonitor.PushReader.*;

public class PushConnector extends UntypedActor {

    static final String CONNECT = "Connect";
    static final String RECONNECT = "Reconnect";
    static final String SHUTDOWN = "Shutdown";
    static final String READER_FAILED = "ReaderFailed";
    private static final ByteString DELIMITER = ByteString.fromString("\n\n");

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private Materializer mat = ActorMaterializer.create(getContext());

    private PushHttp pushHttp;
    private ActorRef pairMatcher;
    private ActorRef reader;
    private Cancellable heartbeatMonitor;
    private UniqueKillSwitch killSwitch;
    private boolean isShuttingDown = false;

    public PushConnector(PushHttp pushHttp, ActorRef pairMatcher) {
        this.pushHttp = pushHttp;
        this.pairMatcher = pairMatcher;
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message.equals(CONNECT)) {
            pushHttp.makeRequest()
                    .whenComplete((response, failure) -> {
                if (failure != null) {
                    log.error("Failed request. Retrying in a few moments...", failure);
                    getContext().system().scheduler().scheduleOnce(Duration.apply(15, TimeUnit.SECONDS), self(), CONNECT, getContext().dispatcher(), self());
                } else {
                    log.info("Connected to push feed.");
                    reader = getContext().actorOf(PushReader.props(pairMatcher), "pushReader");
                    getContext().watch(reader);
                    final Pair<UniqueKillSwitch, NotUsed> killSwitchAndDone = consumeBodyStreamInReader(response);
                    killSwitch = killSwitchAndDone.first();
                    heartbeatMonitor = getContext().system().scheduler().schedule(Duration.apply(1, TimeUnit.MINUTES),
                            Duration.apply(1, TimeUnit.MINUTES), reader, CHECK_HEARTBEAT, getContext().dispatcher(), getSelf());
                }
            });


        } else if (message.equals(RECONNECT)) {
            killSwitch.shutdown();
            heartbeatMonitor.cancel();
            reader.tell(PoisonPill$.MODULE$, self());
            getSelf().tell(CONNECT, getSelf());

        } else if (message.equals(SHUTDOWN)) {
            isShuttingDown = true;
            killSwitch.shutdown();
            heartbeatMonitor.cancel();
            reader.tell(PoisonPill$.MODULE$, self());

        } else if (message.equals(READER_FAILED)) {
            if (!isShuttingDown) {
                self().tell(RECONNECT, self());
            }

        } else if (message instanceof Terminated) {
            log.warning("{} dead.", ((Terminated) message).actor().path());
        }
    }

    private Pair<UniqueKillSwitch, NotUsed> consumeBodyStreamInReader(Source<ByteString, Object> body) {
        return body.viaMat(KillSwitches.single(), Keep.right())
                .via(Framing.delimiter(DELIMITER, 1024, FramingTruncation.ALLOW))
                .toMat(
                        Sink.actorRefWithAck(reader, INIT, ACK, COMPLETE, t -> {
                            log.error(t, "Error following push stream.");
                            return null;
                        }),
                        Keep.both()
                )
                .run(mat);
    }

    public static Props props(final PushHttp pushHttp, final ActorRef pairMatcher) {
        return Props.create(new Creator<PushConnector>() {
            private static final long serialVersionUID = 1L;

            @Override
            public PushConnector create() throws Exception {
                return new PushConnector(pushHttp, pairMatcher);
            }
        });
    }
}
