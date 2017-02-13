package com.ft.notificationsmonitor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.http.javadsl.Http;
import com.ft.notificationsmonitor.PushConnector.Connect$;
import com.ft.notificationsmonitor.PushReader.CancelStreams$;
import com.ft.notificationsmonitor.model.HttpConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.concurrent.duration.Duration;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class NotificationsMonitor {

    private static Logger logger = LoggerFactory.getLogger(NotificationsMonitor.class);

    private ActorSystem sys;
    private ActorRef pushConnector;
    private Cancellable pullSchedule;
    private Cancellable reportSchedule;

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
        ActorRef pairMatcher = sys.actorOf(PairMatcher.props());
        HttpConfig pushHttpConfig = new HttpConfig(config.getString("push-host"), config.getInt("push-port"),
                config.getString("push-uri"), Tuple2.apply(username, password));
        pushConnector = sys.actorOf(PushConnector.props(pushHttpConfig, pairMatcher));
        HttpConfig pullHttpConfig = new HttpConfig(config.getString("pull-host"), config.getInt("pull-port"),
                config.getString("pull-uri"), Tuple2.apply(username, password));
        ActorRef pullConnector = sys.actorOf(PullConnector.props(pullHttpConfig, pairMatcher));
        pullSchedule = sys.scheduler().schedule(Duration.apply(0, TimeUnit.SECONDS) ,
                Duration.apply(1, TimeUnit.MINUTES), pullConnector, "RequestSinceLast", sys.dispatcher(), ActorRef.noSender());
        reportSchedule = sys.scheduler().schedule(Duration.apply(1, TimeUnit.MINUTES),
                Duration.apply(1, TimeUnit.MINUTES), pairMatcher, "Report", sys.dispatcher(), ActorRef.noSender());
        pushConnector.tell(Connect$.MODULE$, ActorRef.noSender());
    }

    private BoxedUnit shutdown() {
        logger.info("Exiting...");
        pullSchedule.cancel();
        reportSchedule.cancel();
        pushConnector.tell(CancelStreams$.MODULE$, ActorRef.noSender());
        Http.get(sys).shutdownAllConnectionPools()
                .whenComplete((s, f) -> sys.terminate());
        return BoxedUnit.UNIT;
    }
}
