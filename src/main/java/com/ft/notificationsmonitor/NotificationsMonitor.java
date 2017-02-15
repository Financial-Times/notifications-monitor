package com.ft.notificationsmonitor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.http.javadsl.Http;
import com.ft.notificationsmonitor.model.HttpConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class NotificationsMonitor {

    private static Logger logger = LoggerFactory.getLogger(NotificationsMonitor.class);

    private ActorSystem sys;
    private ActorRef pushConnector;
    private Cancellable pullSchedule;
    private Cancellable pullSchedule2;
    private Cancellable reportSchedule;
    private Cancellable reportSchedule2;

    public static void main(String[] args) throws Exception {
        NotificationsMonitor monitor = new NotificationsMonitor();
        monitor.run();
        scala.sys.ShutdownHookThread.apply(monitor::shutdown);
    }

    private void run() {
        sys = ActorSystem.create("notifications-monitor");

        logger.info("Starting up...");

        Config sensitiveConfig = ConfigFactory.parseFile(new File("src/main/resources/.sensitive.conf"));
        Config config = ConfigFactory.load().withFallback(sensitiveConfig);
        String username = sensitiveConfig.getString("basic-auth.username");
        String password = sensitiveConfig.getString("basic-auth.password");
        ActorRef pushPullMatcher = sys.actorOf(PairMatcher.props(), "push-pull-matcher");
        ActorRef pullPullMatcher = sys.actorOf(PairMatcher.props(), "pull-pull-matcher");
        HttpConfig pushHttpConfig = new HttpConfig(config.getString("push-host"), config.getInt("push-port"),
                config.getString("push-uri"), username, password);
        pushConnector = sys.actorOf(PushConnector.props(pushHttpConfig, pushPullMatcher));
        HttpConfig pullHttpConfig = new HttpConfig(config.getString("pull-host"), config.getInt("pull-port"),
                config.getString("pull-uri"), username, password);
        ActorRef pullConnector = sys.actorOf(PullConnector.props(pullHttpConfig, Arrays.asList(pushPullMatcher, pullPullMatcher)), "pull-connector-1");
        ActorRef pullConnector2 = sys.actorOf(PullConnector.props(pullHttpConfig, Collections.singletonList(pullPullMatcher)), "pull-connector-2");
        pullSchedule = sys.scheduler().schedule(Duration.apply(0, TimeUnit.SECONDS) ,
                Duration.apply(5, TimeUnit.SECONDS), pullConnector, "RequestSinceLast", sys.dispatcher(), ActorRef.noSender());
        pullSchedule2 = sys.scheduler().schedule(Duration.apply(0, TimeUnit.SECONDS) ,
                Duration.apply(10, TimeUnit.SECONDS), pullConnector2, "RequestSinceLast", sys.dispatcher(), ActorRef.noSender());
        reportSchedule = sys.scheduler().schedule(Duration.apply(3, TimeUnit.MINUTES),
                Duration.apply(3, TimeUnit.MINUTES), pushPullMatcher, "Report", sys.dispatcher(), ActorRef.noSender());
        reportSchedule2 = sys.scheduler().schedule(Duration.apply(0, TimeUnit.SECONDS),
                Duration.apply(10, TimeUnit.SECONDS), pullPullMatcher, "Report", sys.dispatcher(), ActorRef.noSender());
        pushConnector.tell("Connect", ActorRef.noSender());
    }

    private BoxedUnit shutdown() {
        logger.info("Exiting...");
        pullSchedule.cancel();
        pullSchedule2.cancel();
        reportSchedule.cancel();
        reportSchedule2.cancel();
        pushConnector.tell("CancelStreams", ActorRef.noSender());
        Http.get(sys).shutdownAllConnectionPools()
                .whenComplete((s, f) -> sys.terminate());
        return BoxedUnit.UNIT;
    }
}
