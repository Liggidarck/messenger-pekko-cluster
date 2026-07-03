package com.messenger.actor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.messenger.cassandra.CassandraSessionHolder;
import com.messenger.messages.core.ChannelEntityProtocol;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChannelEntityActorTest {

    private static final Config testConfig = ConfigFactory.parseString(
        "pekko.persistence.journal.plugin = \"pekko.persistence.journal.inmem\"\n" +
        "pekko.persistence.snapshot-store.plugin = \"pekko.persistence.snapshot-store.local\""
    ).withFallback(ConfigFactory.load());
    private static final ActorTestKit testKit = ActorTestKit.create(testConfig);
    private static MockedStatic<CassandraSessionHolder> cassandraMock;
    private static CqlSession mockSession;
    private static ClusterSharding mockSharding;

    private final UUID channelId = UUID.randomUUID();
    private final UUID serverId = UUID.randomUUID();
    private final UUID userId = UUID.randomUUID();
    private final UUID otherUserId = UUID.randomUUID();
    private ActorRef<ChannelEntityProtocol.Command> channelEntity;

    @BeforeAll
    static void setupMocks() {
        mockSharding = mock(ClusterSharding.class, Mockito.RETURNS_DEFAULTS);
        EntityRef<?> noopEntityRef = mock(EntityRef.class, Mockito.RETURNS_DEFAULTS);
        when(mockSharding.entityRefFor(any(), any())).thenReturn((EntityRef) noopEntityRef);
        AsyncResultSet emptyResultSet = mock(AsyncResultSet.class);
        when(emptyResultSet.currentPage()).thenReturn(java.util.Collections.emptyList());
        mockSession = mock(CqlSession.class, Mockito.RETURNS_DEFAULTS);
        when(mockSession.executeAsync(any(String.class), any(Object[].class))).thenReturn(
                CompletableFuture.completedFuture(emptyResultSet));

        cassandraMock = Mockito.mockStatic(CassandraSessionHolder.class);
        cassandraMock.when(CassandraSessionHolder::getSession).thenReturn(mockSession);
    }

    @AfterAll
    static void tearDown() {
        cassandraMock.close();
        testKit.shutdownTestKit();
    }

    @BeforeEach
    void setUp() {
        PersistenceId persistenceId = PersistenceId.of("ChannelEntity", channelId.toString());
        Behavior<ChannelEntityProtocol.Command> behavior = Behaviors.setup(ctx ->
                new ChannelEntityActor(persistenceId, ctx, mockSharding, mockSession));
        channelEntity = testKit.spawn(behavior);
    }

    @Test
    void createServerChannelSucceeds() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);

        channelEntity.tell(new ChannelEntityProtocol.CreateChannel(
                serverId, null, "general", userId, probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.ChannelCreated.class, response);
        assertEquals(channelId, ((ChannelEntityProtocol.ChannelCreated) response).channelId());
    }

    @Test
    void createDirectChannelSucceeds() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);

        channelEntity.tell(new ChannelEntityProtocol.CreateChannel(
                null, java.util.Set.of(userId, otherUserId), "DM", userId, probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.ChannelCreated.class, response);
    }

    @Test
    void createDuplicateChannelReturnsError() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);
        channelEntity.tell(new ChannelEntityProtocol.CreateChannel(
                serverId, null, "general", userId, probe.ref()));
        probe.receiveMessage();

        channelEntity.tell(new ChannelEntityProtocol.CreateChannel(
                serverId, null, "dup", userId, probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.ErrorResponse.class, response);
        assertEquals("ALREADY_EXISTS", ((ChannelEntityProtocol.ErrorResponse) response).code());
    }

    @Test
    void sendMessageToCreatedChannelSucceeds() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);
        givenCreatedChannel(probe);

        channelEntity.tell(new ChannelEntityProtocol.SendMessage(
                channelId, userId, "hello", List.of(), null, probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.MessageSent.class, response);
        var sent = (ChannelEntityProtocol.MessageSent) response;
        assertNotNull(sent.messageId());
        assertTrue(sent.createdAt() > 0);
    }

    @Test
    void sendMessageToNonExistentChannelReturnsError() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);

        channelEntity.tell(new ChannelEntityProtocol.SendMessage(
                channelId, userId, "hello", List.of(), null, probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.ErrorResponse.class, response);
        assertEquals("CHANNEL_NOT_FOUND", ((ChannelEntityProtocol.ErrorResponse) response).code());
    }

    @Test
    void editMessageSucceeds() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);
        givenCreatedChannel(probe);
        channelEntity.tell(new ChannelEntityProtocol.SendMessage(
                channelId, userId, "original", List.of(), null, probe.ref()));
        var sent = (ChannelEntityProtocol.MessageSent) probe.receiveMessage();

        channelEntity.tell(new ChannelEntityProtocol.EditMessage(
                channelId, sent.messageId(), userId, "edited", probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.MessageEdited.class, response);
        assertEquals(sent.messageId(), ((ChannelEntityProtocol.MessageEdited) response).messageId());
    }

    @Test
    void editMessageInNonExistentChannelReturnsError() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);

        channelEntity.tell(new ChannelEntityProtocol.EditMessage(
                channelId, UUID.randomUUID(), userId, "edited", probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.ErrorResponse.class, response);
    }

    @Test
    void deleteMessageSucceeds() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);
        givenCreatedChannel(probe);
        channelEntity.tell(new ChannelEntityProtocol.SendMessage(
                channelId, userId, "to-delete", List.of(), null, probe.ref()));
        var sent = (ChannelEntityProtocol.MessageSent) probe.receiveMessage();

        channelEntity.tell(new ChannelEntityProtocol.DeleteMessage(
                channelId, sent.messageId(), userId, probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.MessageDeleted.class, response);
    }

    @Test
    void deleteMessageInNonExistentChannelReturnsError() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);

        channelEntity.tell(new ChannelEntityProtocol.DeleteMessage(
                channelId, UUID.randomUUID(), userId, probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.ErrorResponse.class, response);
    }

    @Test
    void getHistoryOnNonExistentChannelReturnsError() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);

        channelEntity.tell(new ChannelEntityProtocol.GetHistory(10, null, probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.ErrorResponse.class, response);
    }

    @Test
    void typingInNonExistentChannelIsIgnored() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);

        channelEntity.tell(new ChannelEntityProtocol.Typing(userId, probe.ref()));

        probe.expectNoMessage();
    }

    @Test
    void sendMultipleMessagesAndVerifyHistory() {
        var probe = testKit.createTestProbe(ChannelEntityProtocol.Response.class);
        givenCreatedChannel(probe);

        channelEntity.tell(new ChannelEntityProtocol.SendMessage(
                channelId, userId, "first", List.of(), null, probe.ref()));
        probe.receiveMessage();
        channelEntity.tell(new ChannelEntityProtocol.SendMessage(
                channelId, userId, "second", List.of("http://img.com"), null, probe.ref()));
        probe.receiveMessage();

        channelEntity.tell(new ChannelEntityProtocol.GetHistory(10, null, probe.ref()));

        ChannelEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ChannelEntityProtocol.HistoryResponse.class, response);
    }

    private void givenCreatedChannel(
            org.apache.pekko.actor.testkit.typed.javadsl.TestProbe<ChannelEntityProtocol.Response> probe) {
        channelEntity.tell(new ChannelEntityProtocol.CreateChannel(
                serverId, null, "test-channel", userId, probe.ref()));
        probe.receiveMessage();
    }
}
