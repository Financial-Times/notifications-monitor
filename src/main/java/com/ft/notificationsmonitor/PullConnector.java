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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.TimeUnit.MINUTES;

public class PullConnector extends UntypedActor {

    private static final String CONTINUE_REQUESTING_SINCE_LAST = "ContinueRequestingSinceLast";
    static final String REQUEST_SINCE_LAST = "RequestSinceLast";

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private PullHttp pullHttp;
    private List<ActorRef> pairMatchers;
    private Query lastQuery = Query.create(new Pair<>("since", ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT)));

    public PullConnector(PullHttp pullHttp, List<ActorRef> pairMatchers) {
        this.pullHttp = pullHttp;
        this.pairMatchers = pairMatchers;
    }

    @Override
    public void onReceive(Object message) {
        if (message.equals(REQUEST_SINCE_LAST)) {
            makeRequestsUntilEmpty(true);
        } else if (message.equals(CONTINUE_REQUESTING_SINCE_LAST)) {
            makeRequestsUntilEmpty(false);
        }
    }

    private void makeRequestsUntilEmpty(final boolean firstInSeries) {
        CompletionStage<PullPage> pageF = pullHttp.makeRequest(lastQuery);
        pageF.whenComplete((page, failure) -> {
            if (failure != null) {
                log.error(failure, "Failed notifications pull request.");
                scheduleNextRequest();
            } else {
                parseNotificationEntries(page, firstInSeries);
                parseLinkAndScheduleNextRequest(page);
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
                log.info("id={} tid={} lastModified=\"{}\"", entry.id(), entry.publishReference(), entry.lastModified().format(DateTimeFormatter.ISO_INSTANT));
                final DatedEntry datedEntry = new DatedEntry(entry, ZonedDateTime.now());
                pairMatchers.forEach(pairMatcher -> pairMatcher.tell(datedEntry, self()));
            });
        }
    }

    private void parseLinkAndScheduleNextRequest(final PullPage page) {
        JavaConverters.asJavaCollection(page.links())
                .stream()
                .findFirst()
                .map(link -> Uri.create(link.href()).query())
                .ifPresent(query -> {
                    if (!query.equals(lastQuery)) {
                        this.lastQuery = query;
                        getSelf().tell(CONTINUE_REQUESTING_SINCE_LAST, getSelf());
                    } else {
                        scheduleNextRequest();
                    }
                });
    }

    private void scheduleNextRequest() {
        getContext().system().scheduler().scheduleOnce(Duration.apply(2, MINUTES),
                self(), REQUEST_SINCE_LAST, getContext().dispatcher(), self());
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
