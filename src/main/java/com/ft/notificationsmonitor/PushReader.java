package com.ft.notificationsmonitor;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.PoisonPill$;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.NotificationFormats;
import com.ft.notificationsmonitor.model.Read;
import spray.json.JsValue;
import spray.json.JsonParser;
import spray.json.ParserInput;

import java.time.ZonedDateTime;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class PushReader extends UntypedActor {

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private Materializer mat = ActorMaterializer.create(getContext());

    private ActorRef pairMatcher;
    private CompletableFuture<Done> willCancelStreamP;
    private ZonedDateTime heartbeat = ZonedDateTime.now();

    public PushReader(ActorRef pairMatcher) {
        this.pairMatcher = pairMatcher;
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof Read) {
            Read read = (Read) message;
            Source<ByteString, Object> body = read.getBody();
            final Pair<UniqueKillSwitch, CompletionStage<Done>> killSwitchAndDone = consumeBodyStream(body);
            final UniqueKillSwitch killSwitch = killSwitchAndDone.first();
            CompletionStage<Done> done = killSwitchAndDone.second();
            willCancelStreamP = new CompletableFuture<>();
            willCancelStreamP.thenAccept(d -> {
                killSwitch.shutdown();

            });
            done.thenAccept(d -> {
                log.info("Stream has ended.");
                context().parent().tell("StreamEnded", self());
            });
        } else if (message.equals("CheckHeartbeat")) {
            if (heartbeat.isBefore(ZonedDateTime.now().minusMinutes(1))) {
                log.warning("Missed out heartbeats for more than 1 minutes. Cancelling current stream and reconnecting...");
                willCancelStreamP.complete(Done.getInstance());
            }
        } else if (message.equals("CancelStream")) {
            killSwitch.
            willCancelStreamP.complete(Done.getInstance());
        }
    }

    private Pair<UniqueKillSwitch, CompletionStage<Done>> consumeBodyStream(Source<ByteString, Object> body) {
        return body.viaMat(KillSwitches.single(), Keep.right())
                .fold(ByteString.empty(), this::foldPerLine)
                .toMat(Sink.ignore(), Keep.both())
                .run(mat);
    }

    private ByteString foldPerLine(ByteString acc, ByteString next) {
        String nextString = next.utf8String();
        Integer i = nextString.lastIndexOf("\n\n");
        if (i == -1) {
            System.out.print(nextString + "|");
            return acc.concat(next);
        } else {
            List<String> lines = Arrays.asList(nextString.split("\n\n"));
            if (nextString.length() - 2 == i) {
                parseLines(lines);
                return ByteString.empty();
            } else {
                parseLines(lines.subList(0, lines.size() - 1));
                return ByteString.fromString(lines.get(lines.size() - 1));
            }
        }
    }

    private void parseLines(List<String> lines) {
        lines.stream().map(line -> {
            if (line.startsWith("data: [")) {
                return line.substring(7, line.length() - 1);
            }
            return line;
        }).forEach( line -> {
            heartbeat = ZonedDateTime.now();
            if (line.isEmpty()) {
                log.info("heartbeat");
            } else {
                parseLine(line);
            }
        });
    }

    private void parseLine(final String line) {
        CompletableFuture.supplyAsync(() -> {
            JsValue parser = JsonParser.apply(new ParserInput.StringBasedParserInput(line));
            return parser.convertTo(NotificationFormats.pushEntryFormat());
        }).whenComplete((entry, failure) -> {
            if (failure != null) {
                log.error(failure, "Error deserializing notifications response");
            } else {
                log.info(entry.id());
                pairMatcher.tell(new DatedEntry(entry, ZonedDateTime.now()), self());
            }
        });
    }

    public static Props props(final ActorRef pairMatcher) {
        return Props.create(new Creator<PushReader>() {
            private static final long serialVersionUID = 1L;

            @Override
            public PushReader create() throws Exception {
                return new PushReader(pairMatcher);
            }
        });
    }
}
