package com.ft.notificationsmonitor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import com.ft.notificationsmonitor.http.PullHttp;
import com.ft.notificationsmonitor.http.PushHttp;
import com.ft.notificationsmonitor.model.HttpConfig;
import com.ft.notificationsmonitor.model.PushHttpConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;
import scala.runtime.BoxedUnit;

import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.ft.notificationsmonitor.PullConnector.REQUEST_SINCE_LAST;
import static com.ft.notificationsmonitor.PushConnector.CONNECT;
import static com.ft.notificationsmonitor.PushConnector.SHUTDOWN;

public class NotificationsMonitor {

    private static Logger logger = LoggerFactory.getLogger(NotificationsMonitor.class);

    private ActorSystem sys;
    private ActorRef pushConnector;
    private Cancellable pushPullMatcherReport;
    private CompletionStage<ServerBinding> serverBinding;

    public static void main(String[] args) throws Exception {
        NotificationsMonitor monitor = new NotificationsMonitor();
        monitor.run();
        scala.sys.ShutdownHookThread.apply(monitor::shutdown);
    }

    private void run() {
        sys = ActorSystem.create("notificationsMonitor");
        logger.info("Starting up...");
        final Config config = ConfigFactory.load(
                ConfigFactory.parseResources("application.conf")
                        .withFallback(ConfigFactory.parseResources("sensitive.conf"))
                        .resolve()
        );
        ActorRef pushPullMatcher = sys.actorOf(PairMatcher.props("push", "pull"), "matcherPushPull");
        PushHttpConfig pushHttpConfig = new PushHttpConfig(config.getString("push-host"),
                config.getInt("push-port"),
                config.getString("push-uri"),
                config.getString("delivery.basic-auth.username"),
                config.getString("delivery.basic-auth.password"),
                config.getString("delivery.apiKey"));
        PushHttp pushHttp = new PushHttp(sys, pushHttpConfig);
        pushConnector = sys.actorOf(PushConnector.props(pushHttp, pushPullMatcher), "pushConnector");
        HttpConfig pullHttpConfig = new HttpConfig(config.getString("pull-host"),
                config.getInt("pull-port"),
                config.getString("pull-uri"),
                config.getString("delivery.basic-auth.username"),
                config.getString("delivery.basic-auth.password"));
        PullHttp pullHttp = new PullHttp(sys, pullHttpConfig);
        ActorRef pullConnector = sys.actorOf(PullConnector.props(pullHttp, Collections.singletonList(pushPullMatcher)), "pullConnector");
        pullConnector.tell(REQUEST_SINCE_LAST, ActorRef.noSender());
        pushPullMatcherReport = sys.scheduler().schedule(Duration.apply(250, TimeUnit.SECONDS),
                Duration.apply(4, TimeUnit.MINUTES), pushPullMatcher, "Report", sys.dispatcher(), ActorRef.noSender());
        pushConnector.tell(CONNECT, ActorRef.noSender());

        serverBinding = (new HttpServer(sys)).runServer();
        logger.info("Serving HTTP on 0.0.0.0:8080");
    }

    private BoxedUnit shutdown() {
        logger.info("Exiting...");
        pushPullMatcherReport.cancel();
        try {
            serverBinding.thenCompose(ServerBinding::unbind).toCompletableFuture().get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException | InterruptedException ex ) {
            logger.error("Error while shutting down HTTP server", ex);
        }
        pushConnector.tell(SHUTDOWN, ActorRef.noSender());
        Http.get(sys).shutdownAllConnectionPools()
                .whenComplete((s, f) -> sys.terminate());
        return BoxedUnit.UNIT;
    }
}
