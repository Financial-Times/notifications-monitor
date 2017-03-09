package com.ft.notificationsmonitor;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import com.ft.notificationsmonitor.http.NativeHttp;
import com.ft.notificationsmonitor.http.PullHttp;
import com.ft.notificationsmonitor.model.DatedEntry;
import com.ft.notificationsmonitor.model.Link;
import com.ft.notificationsmonitor.model.PullEntry;
import com.ft.notificationsmonitor.model.PullPage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import scala.collection.JavaConverters;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.ft.notificationsmonitor.PullConnector.REQUEST_SINCE_LAST;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PullConnectorTest {

    private static ActorSystem sys;

    @BeforeClass
    public static void setup() {
        sys = ActorSystem.create("notifications-monitor-test");
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
            NativeHttp mockNativeHttp = Mockito.mock(NativeHttp.class);
            final JavaTestKit probe = new JavaTestKit(sys);
            final TestActorRef<PullConnector> pullConnectorRef = TestActorRef.create(sys, PullConnector.props(mockHttp, mockNativeHttp, Collections.singletonList(probe.getRef())), "pullConnector1");
            when(mockHttp.makeRequest(any(), anyString())).thenReturn(CompletableFuture.completedFuture(setupTestPage()));
            when(mockNativeHttp.getNativeContent(any())).thenReturn(CompletableFuture.completedFuture(Optional.of("native")));

            pullConnectorRef.tell(REQUEST_SINCE_LAST, getRef());

            verify(mockHttp, times(2)).makeRequest(any(), anyString());
            verify(mockNativeHttp, times(2)).getNativeContent(anyString());
            probe.expectMsgClass(DatedEntry.class);
        }};
    }

    private PullPage setupTestPage() {
        List<PullEntry> entries = Collections.singletonList(new PullEntry("a", "p", ZonedDateTime.now(), ZonedDateTime.now()));
        List<Link> links = Collections.singletonList(new Link("https://prod-uk.ft.com/content/notifications?since=123"));
        scala.collection.immutable.List<PullEntry> entriesS = JavaConverters.asScalaBuffer(entries).toList();
        scala.collection.immutable.List<Link> linksS = JavaConverters.asScalaBuffer(links).toList();
        return new PullPage(entriesS, linksS);
    }

    @Test
    public void testUnsuccessfulRequestIsHandled() {
        new JavaTestKit(sys) {{
            PullHttp mockHttp = Mockito.mock(PullHttp.class);
            NativeHttp mockNativeHttp = Mockito.mock(NativeHttp.class);
            final JavaTestKit probe = new JavaTestKit(sys);
            final TestActorRef<PullConnector> pullConnectorRef = TestActorRef.create(sys, PullConnector.props(mockHttp, mockNativeHttp, Collections.singletonList(probe.getRef())), "pullConnector2");
            doThrow(new RuntimeException("Problem in request")).when(mockHttp).makeRequest(any(), anyString());

            pullConnectorRef.tell(REQUEST_SINCE_LAST, getRef());

            verify(mockHttp).makeRequest(any(), anyString());
            verifyNoMoreInteractions(mockNativeHttp);
            probe.expectNoMsg();
        }};
    }

    @Test
    public void testSuccessfulRequestButVerifiedPlaceholderIsNotSentToMatcher() {
        new JavaTestKit(sys) {{
            PullHttp mockHttp = Mockito.mock(PullHttp.class);
            NativeHttp mockNativeHttp = Mockito.mock(NativeHttp.class);
            final JavaTestKit probe = new JavaTestKit(sys);
            final TestActorRef<PullConnector> pullConnectorRef = TestActorRef.create(sys, PullConnector.props(mockHttp, mockNativeHttp, Collections.singletonList(probe.getRef())), "pullConnector3");
            when(mockHttp.makeRequest(any(), anyString())).thenReturn(CompletableFuture.completedFuture(setupTestPage()));
            when(mockNativeHttp.getNativeContent(any())).thenReturn(CompletableFuture.completedFuture(Optional.of("ContentPlaceholder")));

            pullConnectorRef.tell(REQUEST_SINCE_LAST, getRef());

            verify(mockHttp, times(2)).makeRequest(any(), anyString());
            verify(mockNativeHttp, times(2)).getNativeContent(anyString());
            probe.expectNoMsg();
        }};
    }

    @Test
    public void testSuccessfulRequestAndPlaceholderVerificationUnsuccessfulThenSentToMatcher() {
        new JavaTestKit(sys) {{
            PullHttp mockHttp = Mockito.mock(PullHttp.class);
            NativeHttp mockNativeHttp = Mockito.mock(NativeHttp.class);
            final JavaTestKit probe = new JavaTestKit(sys);
            final TestActorRef<PullConnector> pullConnectorRef = TestActorRef.create(sys, PullConnector.props(mockHttp, mockNativeHttp, Collections.singletonList(probe.getRef())), "pullConnector4");
            when(mockHttp.makeRequest(any(), anyString())).thenReturn(CompletableFuture.completedFuture(setupTestPage()));
            when(mockNativeHttp.getNativeContent(any())).thenReturn(
                    CompletableFuture.supplyAsync(() -> {
                        throw new RuntimeException("Placeholder verification failed.");
                    })
            );

            pullConnectorRef.tell(REQUEST_SINCE_LAST, getRef());

            verify(mockHttp, times(2)).makeRequest(any(), anyString());
            verify(mockNativeHttp, times(2)).getNativeContent(anyString());
            probe.expectMsgClass(DatedEntry.class);
        }};
    }
}
