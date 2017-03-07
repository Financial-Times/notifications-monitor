package com.ft.notificationsmonitor;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import com.ft.notificationsmonitor.http.PlaceholderSkipper;
import com.ft.notificationsmonitor.http.PullHttp;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.PullPage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static com.ft.notificationsmonitor.PullConnector.REQUEST_SINCE_LAST;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
            PlaceholderSkipper mockPlaceholderSkipper = Mockito.mock(PlaceholderSkipper.class);
            final JavaTestKit probe = new JavaTestKit(sys);
            final TestActorRef<PullConnector> pullConnectorRef = TestActorRef.create(sys, PullConnector.props(mockHttp, mockPlaceholderSkipper, Collections.singletonList(probe.getRef())), "pullConnector");
            when(mockHttp.makeRequest(any())).thenReturn(CompletableFuture.completedFuture(new PullPage(null, null)));
            when(mockPlaceholderSkipper.shouldSkip(any())).thenReturn(CompletableFuture.completedFuture(Boolean.FALSE));

            pullConnectorRef.tell(REQUEST_SINCE_LAST, getRef());

            verify(mockHttp).makeRequest(any());
            verify(mockPlaceholderSkipper).shouldSkip(anyString());
            probe.expectMsgClass(DatedEntry.class);
        }};
    }

    @Test
    public void testUnsuccessfulRequestIsHandled() {
        new JavaTestKit(sys) {{
            PullHttp mockHttp = Mockito.mock(PullHttp.class);
            PlaceholderSkipper mockPlaceholderSkipper = Mockito.mock(PlaceholderSkipper.class);
            final JavaTestKit probe = new JavaTestKit(sys);
            final TestActorRef<PullConnector> pullConnectorRef = TestActorRef.create(sys, PullConnector.props(mockHttp, mockPlaceholderSkipper, Collections.singletonList(probe.getRef())), "pullConnector");
            doThrow(new RuntimeException("Problem in request")).when(mockHttp).makeRequest(any());

            pullConnectorRef.tell(REQUEST_SINCE_LAST, getRef());

            verify(mockHttp).makeRequest(any());
            verifyNoMoreInteractions(mockPlaceholderSkipper);
            probe.expectNoMsg();
        }};
    }

    @Test
    public void testSuccessfulRequestButVerifiedPlaceholderIsNotSentToMatcher() {
        new JavaTestKit(sys) {{
            PullHttp mockHttp = Mockito.mock(PullHttp.class);
            PlaceholderSkipper mockPlaceholderSkipper = Mockito.mock(PlaceholderSkipper.class);
            final JavaTestKit probe = new JavaTestKit(sys);
            final TestActorRef<PullConnector> pullConnectorRef = TestActorRef.create(sys, PullConnector.props(mockHttp, mockPlaceholderSkipper, Collections.singletonList(probe.getRef())), "pullConnector");
            when(mockHttp.makeRequest(any())).thenReturn(CompletableFuture.completedFuture(new PullPage(null, null)));
            when(mockPlaceholderSkipper.shouldSkip(any())).thenReturn(CompletableFuture.completedFuture(Boolean.TRUE));

            pullConnectorRef.tell(REQUEST_SINCE_LAST, getRef());

            verify(mockHttp).makeRequest(any());
            verify(mockPlaceholderSkipper).shouldSkip(anyString());
            probe.expectNoMsg();
        }};
    }
}
