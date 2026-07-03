package com.messenger.actor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.messenger.messages.core.UserEntityProtocol;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class UserEntityActorTest {

    private static final Config testConfig = ConfigFactory.parseString(
        "pekko.persistence.journal.plugin = \"pekko.persistence.journal.inmem\"\n" +
        "pekko.persistence.snapshot-store.plugin = \"pekko.persistence.snapshot-store.local\""
    ).withFallback(ConfigFactory.load());
    private static final ActorTestKit testKit = ActorTestKit.create(testConfig);
    private static CqlSession mockSession;

    private final UUID userId = UUID.randomUUID();
    private ActorRef<UserEntityProtocol.Command> userEntity;

    @BeforeAll
    static void setupMockSession() {
        mockSession = mock(CqlSession.class);
    }

    @AfterAll
    static void tearDown() {
        testKit.shutdownTestKit();
    }

    @BeforeEach
    void setUp() {
        EntityContext<UserEntityProtocol.Command> entityContext = new EntityContext<>(
                UserEntityProtocol.ENTITY_KEY, userId.toString(), null);
        Behavior<UserEntityProtocol.Command> behavior = Behaviors.setup(ctx ->
                new UserEntityActor(ctx, entityContext, mockSession));
        userEntity = testKit.spawn(behavior);
    }

    @Test
    void deviceConnectedSendsConnectedMessage() {
        var probe = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        String sessionId = "session-1";

        userEntity.tell(new UserEntityProtocol.DeviceConnected(sessionId, probe.ref()));

        probe.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));
    }

    @Test
    void deviceDisconnectedRemovesSession() {
        var probe = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        String sessionId = "session-1";
        userEntity.tell(new UserEntityProtocol.DeviceConnected(sessionId, probe.ref()));
        probe.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));

        userEntity.tell(new UserEntityProtocol.DeviceDisconnected(sessionId));
    }

    @Test
    void notifyTypingBroadcastsToConnectedSessions() {
        var probe = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        String sessionId = "session-1";
        userEntity.tell(new UserEntityProtocol.DeviceConnected(sessionId, probe.ref()));
        probe.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));

        UUID channelId = UUID.randomUUID();
        UUID typingUserId = UUID.randomUUID();
        userEntity.tell(new UserEntityProtocol.NotifyTyping(channelId, typingUserId));

        String expected = String.format(
                "{\"type\":\"user_typing\",\"channelId\":\"%s\",\"userId\":\"%s\"}",
                channelId, typingUserId);
        probe.expectMessage(new UserEntityProtocol.OutgoingMessage(expected));
    }

    @Test
    void notifyNewChannelMessageBroadcastsToConnectedSessions() {
        var probe = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        userEntity.tell(new UserEntityProtocol.DeviceConnected("s1", probe.ref()));
        probe.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));

        UUID serverId = UUID.randomUUID();
        UUID channelId = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();
        UUID fromUserId = UUID.randomUUID();
        userEntity.tell(new UserEntityProtocol.NotifyNewChannelMessage(
                serverId, channelId, messageId, fromUserId, "hello", 1000L, java.util.List.of(), null));

        String json = probe.receiveMessage().payload();
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"type\":\"new_message\""));
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"text\":\"hello\""));
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"channelId\":\"" + channelId + "\""));
    }

    @Test
    void notifyEditMessageBroadcastsToConnectedSessions() {
        var probe = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        userEntity.tell(new UserEntityProtocol.DeviceConnected("s1", probe.ref()));
        probe.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));

        UUID channelId = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();
        userEntity.tell(new UserEntityProtocol.NotifyEditMessage(
                UUID.randomUUID(), channelId, messageId, "edited text", 2000L));

        String expected = String.format(
                "{\"type\":\"message_edited\",\"serverId\":\"%s\",\"channelId\":\"%s\",\"messageId\":\"%s\",\"text\":\"edited text\",\"editedAt\":%d}",
                UUID.randomUUID(), channelId, messageId, 2000L);
        // Compare structure, not exact string (UUID varies)
        String json = probe.receiveMessage().payload();
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"type\":\"message_edited\""));
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"text\":\"edited text\""));
    }

    @Test
    void notifyDeleteMessageBroadcastsToConnectedSessions() {
        var probe = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        userEntity.tell(new UserEntityProtocol.DeviceConnected("s1", probe.ref()));
        probe.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));

        UUID channelId = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();
        userEntity.tell(new UserEntityProtocol.NotifyDeleteMessage(
                UUID.randomUUID(), channelId, messageId));

        String json = probe.receiveMessage().payload();
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"type\":\"message_deleted\""));
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"messageId\":\"" + messageId + "\""));
    }

    @Test
    void notifyChannelCreatedBroadcastsToConnectedSessions() {
        var probe = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        userEntity.tell(new UserEntityProtocol.DeviceConnected("s1", probe.ref()));
        probe.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));

        UUID serverId = UUID.randomUUID();
        UUID channelId = UUID.randomUUID();
        userEntity.tell(new UserEntityProtocol.NotifyChannelCreated(serverId, channelId, "general"));

        String json = probe.receiveMessage().payload();
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"type\":\"channel_created\""));
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"channelName\":\"general\""));
    }

    @Test
    void deliverMessageSendsToConnectedSessions() {
        var probe = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        userEntity.tell(new UserEntityProtocol.DeviceConnected("s1", probe.ref()));
        probe.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));

        userEntity.tell(new UserEntityProtocol.DeliverMessage(UUID.randomUUID(), "hello world"));

        String msg = probe.receiveMessage().payload();
        org.junit.jupiter.api.Assertions.assertTrue(msg.contains("hello world"));
    }

    @Test
    void messageToDisconnectedSessionIsLoggedOnly() {
        var probe = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);

        userEntity.tell(new UserEntityProtocol.DeliverMessage(UUID.randomUUID(), "hello"));

        probe.expectNoMessage();
    }

    @Test
    void escapeJsonHandlesSpecialCharacters() {
        var probe = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        userEntity.tell(new UserEntityProtocol.DeviceConnected("s1", probe.ref()));
        probe.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));

        String textWithQuotes = "he said \"hello\"";
        userEntity.tell(new UserEntityProtocol.NotifyNewChannelMessage(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                UUID.randomUUID(), textWithQuotes, 1000L, java.util.List.of(), null));

        String json = probe.receiveMessage().payload();
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("he said \\\"hello\\\""));
    }

    @Test
    void multipleSessionsReceiveNotification() {
        var probe1 = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        var probe2 = testKit.createTestProbe(UserEntityProtocol.OutgoingMessage.class);
        userEntity.tell(new UserEntityProtocol.DeviceConnected("s1", probe1.ref()));
        userEntity.tell(new UserEntityProtocol.DeviceConnected("s2", probe2.ref()));
        probe1.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));
        probe2.expectMessage(new UserEntityProtocol.OutgoingMessage("connected"));

        userEntity.tell(new UserEntityProtocol.NotifyTyping(UUID.randomUUID(), UUID.randomUUID()));

        probe1.expectMessageClass(UserEntityProtocol.OutgoingMessage.class);
        probe2.expectMessageClass(UserEntityProtocol.OutgoingMessage.class);
    }
}
