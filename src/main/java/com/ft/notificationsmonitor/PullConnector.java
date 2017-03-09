package com.ft.notificationsmonitor;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.http.javadsl.model.Query;
import akka.http.javadsl.model.Uri;
import akka.japi.Creator;
import akka.japi.Pair;
import com.ft.notificationsmonitor.http.NativeHttp;
import com.ft.notificationsmonitor.http.PullHttp;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.PullEntry;
import com.ft.notificationsmonitor.model.PullPage;
import scala.collection.JavaConverters;
import scala.concurrent.duration.Duration;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.model.HttpCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.util.concurrent.TimeUnit.MINUTES;

public class PullConnector extends UntypedActor {

    private static final String CONTINUE_REQUESTING_SINCE_LAST = "ContinueRequestingSinceLast";
    static final String REQUEST_SINCE_LAST = "RequestSinceLast";

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private PullHttp pullHttp;
    private NativeHttp nativeHttp;
    private List<ActorRef> pairMatchers;
    private Query lastQuery = Query.create(new Pair<>("since", ZonedDateTime.now().format(ISO_INSTANT)));

    public PullConnector(final PullHttp pullHttp, final NativeHttp nativeHttp, final List<ActorRef> pairMatchers) {
        this.pullHttp = pullHttp;
        this.nativeHttp = nativeHttp;
        this.pairMatchers = pairMatchers;
    }

    @Override
    public void onReceive(Object message) {
        if (message.equals(REQUEST_SINCE_LAST)) {
            pullUntilEmpty(true);
        } else if (message.equals(CONTINUE_REQUESTING_SINCE_LAST)) {
            pullUntilEmpty(false);
        }
    }

    private void pullUntilEmpty(final boolean firstInSeries) {
        final String tid = UUID.randomUUID().toString();
        log.debug("Making pull request. query=\"{}\" tid={}", lastQuery.render(UTF_8), tid);
        CompletionStage<PullPage> pageF = pullHttp.makeRequest(lastQuery, tid);
        pageF.whenComplete((page, failure) -> {
            if (failure != null) {
                log.error(failure, "Failed notifications pull request.");
                scheduleNextPull();
            } else {
                parseNotificationEntries(page, firstInSeries);
                parseLinkAndScheduleNextPull(page);
            }
        });
    }

    private void parseNotificationEntries(final PullPage page, final boolean firstInSeries) {
        final Collection<PullEntry> notifications = JavaConverters.asJavaCollection(page.notifications());
        if (notifications.isEmpty()) {
            if (firstInSeries) {
                log.info("heartbeat");
            }
        } else {
            notifications.forEach(entry -> {
                final DatedEntry datedEntry = new DatedEntry(entry, ZonedDateTime.now());
                shouldSkipBecauseItsPlaceholder(entry).thenAccept(shouldSkip -> {
                    if (shouldSkip) {
                        log.info("ContentPlaceholder id={} tid={} lastModified=\"{}\"", entry.id(), entry.publishReference(), entry.lastModified().format(ISO_INSTANT));
                    } else {
                        log.info("id={} tid={} lastModified=\"{}\" notificationDate=\"{}\"", entry.id(), entry.publishReference(), entry.lastModified().format(ISO_INSTANT), entry.notificationDate().format(ISO_INSTANT));
                        pairMatchers.forEach(pairMatcher -> pairMatcher.tell(datedEntry, self()));
                    }
                });
            });
        }
    }

    private CompletionStage<Boolean> shouldSkipBecauseItsPlaceholder(final PullEntry entry) {
        return nativeHttp.getNativeContent(entry.id())
                .exceptionally(ex -> {
                    log.error(ex, "Failed getting native content to verify if it's placeholder for id={}", entry.id());
                    return Optional.empty();
                })
                .thenApply(nativeContentO -> nativeContentO.isPresent() && nativeContentO.get().contains("ContentPlaceholder"));
    }

    private void parseLinkAndScheduleNextPull(final PullPage page) {
        JavaConverters.asJavaCollection(page.links())
                .stream()
                .findFirst()
                .map(link -> Uri.create(link.href()).query())
                .ifPresent(query -> {
                    if (!query.equals(lastQuery)) {
                        this.lastQuery = query;
                        getSelf().tell(CONTINUE_REQUESTING_SINCE_LAST, getSelf());
                    } else {
                        scheduleNextPull();
                    }
                });
    }

    private void scheduleNextPull() {
        getContext().system().scheduler().scheduleOnce(Duration.apply(2, MINUTES),
                self(), REQUEST_SINCE_LAST, getContext().dispatcher(), self());
    }

    public static Props props(final PullHttp pullHttp, final NativeHttp nativeHttp, final List<ActorRef> pairMatchers) {
        return Props.create(new Creator<PullConnector>() {
            private static final long serialVersionUID = 1L;

            @Override
            public PullConnector create() throws Exception {
                return new PullConnector(pullHttp, nativeHttp, pairMatchers);
            }
        });
    }
}
