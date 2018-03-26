package com.ft.notificationsmonitor;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Status.Failure;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import akka.util.ByteString;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.NotificationFormats;
import spray.json.JsValue;
import spray.json.JsonParser;
import spray.json.ParserInput;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ft.notificationsmonitor.PushConnector.READER_FAILED;
import static com.ft.notificationsmonitor.PushConnector.RECONNECT;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;

public class PushReader extends UntypedActor {

    static final String INIT = "Init";
    static final String ACK = "Ack";
    static final String COMPLETE = "Complete";
    static final String CHECK_HEARTBEAT = "CheckHeartbeat";
    static final Pattern INNER_JSON_ENTRY = Pattern.compile(".*data: \\[(.*)\\].*");

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private ActorRef pairMatcher;
    private ZonedDateTime heartbeat = ZonedDateTime.now();

    public PushReader(ActorRef pairMatcher) {
        this.pairMatcher = pairMatcher;
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof ByteString) {
            parseBytes((ByteString) message);
            sender().tell(ACK, self());

        } else if (message.equals(CHECK_HEARTBEAT)) {
            if (heartbeat.isBefore(ZonedDateTime.now().minusMinutes(1))) {
                log.warning("Missed out heartbeats for more than 1 minutes.");
                context().parent().tell(RECONNECT, self());
            }

        } else if (message.equals(INIT)) {
            getSender().tell(ACK, self());

        } else if (message.equals(COMPLETE)) {
            log.info("Stream has ended.");
            context().parent().tell(RECONNECT, self());

        } else if (message instanceof Failure) {
            final Failure failure = (Failure) message;
            log.error(failure.cause(), "Stream has failed.");
            getContext().parent().tell(READER_FAILED, getSelf());
        }
    }

    private void parseBytes(final ByteString byteString) {
        final String line = byteString.utf8String();
        final String unwrappedLine;
        Matcher innerJsonMatcher = INNER_JSON_ENTRY.matcher(line);
        if (innerJsonMatcher.find()) {
            unwrappedLine = innerJsonMatcher.group(1);
        } else {
            log.warning("not matching 'data: []' pattern line={}", line);
            return;
        }
        heartbeat = ZonedDateTime.now();
        if (unwrappedLine.isEmpty()) {
            log.info("heartbeat");
        } else {
            parseUnwrappedLine(unwrappedLine);
        }
    }

    private void parseUnwrappedLine(final String line) {
        System.out.println(line);
        CompletableFuture.supplyAsync(() -> {
            JsValue parser = JsonParser.apply(new ParserInput.StringBasedParserInput(line));
            return parser.convertTo(NotificationFormats.pushEntryFormat());
        }).whenComplete((entry, failure) -> {
            if (failure != null) {
                log.error(failure, "Error deserializing notifications response");
            } else {
                final DatedEntry datedEntry = new DatedEntry(entry, ZonedDateTime.now());
                log.info("id={} publishReference={} lastModified=\"{}\" foundAt=\"{}\"",
                        entry.id(),
                        entry.publishReference(),
                        entry.lastModified().format(ISO_INSTANT),
                        datedEntry.getDate().format(ISO_INSTANT));
                pairMatcher.tell(datedEntry, self());
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
