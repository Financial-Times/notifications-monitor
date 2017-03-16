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
import com.ft.notificationsmonitor.http.PullHttp;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.PullEntry;
import com.ft.notificationsmonitor.model.PullPage;
import scala.collection.JavaConverters;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static akka.http.javadsl.model.HttpCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PullConnector extends UntypedActor {

    private static final FiniteDuration POLL_INTERVAL = Duration.apply(5, SECONDS);
    private static final String CONTINUE_REQUESTING_SINCE_LAST = "ContinueRequestingSinceLast";
    static final String REQUEST_SINCE_LAST = "RequestSinceLast";

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private PullHttp pullHttp;
    private List<ActorRef> pairMatchers;
    private Query lastQuery = Query.create(new Pair<>("since", ZonedDateTime.now().format(ISO_INSTANT)));
    private History history = new History();

    public PullConnector(final PullHttp pullHttp, final List<ActorRef> pairMatchers) {
        this.pullHttp = pullHttp;
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
        log.info("Making pull request. query=\"{}\" tid={}", lastQuery.render(UTF_8), tid);
        pullHttp.makeRequest(lastQuery, tid).whenComplete((page, failure) -> {
            if (failure != null) {
                log.error(failure, "Failed notifications pull request. query=\"{}\" tid={}", lastQuery, tid);
                scheduleLaterPull();
            } else {
                parseNotificationEntries(page, firstInSeries, tid);
            }
        });
    }

    private void parseNotificationEntries(final PullPage page, final boolean firstInSeries, final String tid) {
        final Query currentQuery = lastQuery;
        final Collection<PullEntry> notifications = JavaConverters.asJavaCollection(page.notifications());
        parseAndSaveLink(page);
        if (notifications.isEmpty()) {
            if (firstInSeries) {
                log.info("heartbeat");
            }
            scheduleLaterPull();
        } else {
            final ZonedDateTime now = ZonedDateTime.now();
            notifications.forEach(entry -> {
                final DatedEntry datedEntry = new DatedEntry(entry, now);
                if (history.verifyAndAddToHistory(datedEntry)) {
                    log.info(String.format("id=%s publishReference=%s lastModified=\"%s\" notificationDate=\"%s\" query=\"%s\" tid=%s foundAt=\"%s\"",
                            entry.id(),
                            entry.publishReference(),
                            entry.lastModified().format(ISO_INSTANT),
                            entry.notificationDate().format(ISO_INSTANT),
                            currentQuery.render(UTF_8),
                            tid,
                            datedEntry.getDate().format(ISO_INSTANT))
                    );
                    pairMatchers.forEach(pairMatcher -> pairMatcher.tell(datedEntry, self()));
                } else {
                    log.warning(String.format("Duplicate entry. Same id and publishReference was seen in the last 5 minutes. id=%s publishReference=%s lastModified=\"%s\" notificationDate=\"%s\" query=\"%s\" tid=%s foundAt=\"%s\"",
                            entry.id(),
                            entry.publishReference(),
                            entry.lastModified().format(ISO_INSTANT),
                            entry.notificationDate().format(ISO_INSTANT),
                            currentQuery.render(UTF_8),
                            tid,
                            datedEntry.getDate().format(ISO_INSTANT))
                    );
                }
            });
            getSelf().tell(CONTINUE_REQUESTING_SINCE_LAST, getSelf());
        }
        history.clearSomeHistory();
    }

    private void parseAndSaveLink(final PullPage page) {
        JavaConverters.asJavaCollection(page.links())
                .stream()
                .findFirst()
                .map(link -> Uri.create(link.href()).query())
                .ifPresent(query -> {
                    this.lastQuery = query;
                });
    }

    private void scheduleLaterPull() {
        getContext().system().scheduler().scheduleOnce(POLL_INTERVAL, self(), REQUEST_SINCE_LAST, getContext().dispatcher(), self());
    }

    public static Props props(final PullHttp pullHttp, final List<ActorRef> pairMatchers) {
        return Props.create(new Creator<PullConnector>() {
            private static final long serialVersionUID = 1L;

            @Override
            public PullConnector create() throws Exception {
                return new PullConnector(pullHttp, pairMatchers);
            }
        });
    }
}
