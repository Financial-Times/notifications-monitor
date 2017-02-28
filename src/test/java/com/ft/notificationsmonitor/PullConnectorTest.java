package com.ft.notificationsmonitor;

import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.PullEntry;
import com.ft.notificationsmonitor.model.PullPage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.actor.ActorSystem;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static com.ft.notificationsmonitor.PullConnector.REQUEST_SINCE_LAST;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PullConnectorTest {

    private static ActorSystem sys;

    @BeforeClass
    public static void setup() {
        sys = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(sys);
        sys = null;
    }

    @Test
    public void testSuccessfulRequestIsSentToMatcher() {
        new JavaTestKit(sys) {{
            PullHttp mockHttp = Mockito.mock(PullHttp.class);
            final JavaTestKit probe = new JavaTestKit(sys);
            final TestActorRef<PullConnector> pullConnectorRef = TestActorRef.create(sys, PullConnector.props(mockHttp, Collections.singletonList(probe.getRef())), "pullConnector");
            when(mockHttp.makeRequest(any())).thenReturn(CompletableFuture.completedFuture(new PullPage(JavaConverters.asScalaBuffer(Arrays.asList(new PullEntry("a"))).toList())));

            pullConnectorRef.tell(REQUEST_SINCE_LAST, getRef());

            verify(mockHttp).makeRequest(any());
            probe.expectMsgClass(DatedEntry.class);
        }};
    }

    @Test
    public void testUnsuccessfulRequestIsHandled() {
        new JavaTestKit(sys) {{
            PullHttp mockHttp = Mockito.mock(PullHttp.class);
            final JavaTestKit probe = new JavaTestKit(sys);
            final TestActorRef<PullConnector> pullConnectorRef = TestActorRef.create(sys, PullConnector.props(mockHttp, Collections.singletonList(probe.getRef())), "pullConnector");
            doThrow(new RuntimeException("Problem in request")).when(mockHttp).makeRequest(any());

            pullConnectorRef.tell(REQUEST_SINCE_LAST, getRef());

            verify(mockHttp).makeRequest(any());
            probe.expectNoMsg();
        }};
    }
}
