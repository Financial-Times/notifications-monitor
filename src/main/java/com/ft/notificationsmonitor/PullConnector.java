package com.ft.notificationsmonitor;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.PullEntry;
import com.ft.notificationsmonitor.model.PullPage;
import scala.collection.JavaConverters;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class PullConnector extends UntypedActor {

    static final String REQUEST_SINCE_LAST = "RequestSinceLast";

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private PullHttp pullHttp;
    private List<ActorRef> pairMatchers;
    private ZonedDateTime last = ZonedDateTime.now();

    public PullConnector (PullHttp pullHttp, List<ActorRef> pairMatchers) {
        this.pullHttp = pullHttp;
        this.pairMatchers = pairMatchers;
    }

    @Override
    public void onReceive(Object message) {
        if (message.equals(REQUEST_SINCE_LAST)) {
            parseResponse(pullHttp.makeRequest(last));
        }
    }

    private void parseResponse(CompletionStage<PullPage> pageF) {
        pageF.whenComplete((page, failure) -> {
            if (failure != null) {
                log.error(failure, "Failed notifications pull request.");
            } else {
                last = ZonedDateTime.now();
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
