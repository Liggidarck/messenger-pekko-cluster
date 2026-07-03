package com.messenger.actor;

import com.messenger.messages.core.ServerEntityProtocol;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ServerStateTest {

    private final UUID serverId = UUID.randomUUID();
    private final UUID ownerId = UUID.randomUUID();
    private final UUID userId = UUID.randomUUID();
    private final String serverName = "test-server";

    @Test
    void emptyStateIsNotCreated() {
        ServerState state = ServerState.empty(serverId);
        assertFalse(state.isCreated());
        assertNull(state.getServerName());
        assertNull(state.getOwnerId());
        assertTrue(state.getMembers().isEmpty());
        assertTrue(state.getChannels().isEmpty());
    }

    @Test
    void serverCreatedEventCreatesServer() {
        ServerState state = ServerState.empty(serverId);
        long now = System.currentTimeMillis();
        var event = new ServerEntityProtocol.ServerCreatedEvent(serverId, serverName, ownerId, now);

        state = state.applyEvent(event);

        assertTrue(state.isCreated());
        assertEquals(serverName, state.getServerName());
        assertEquals(ownerId, state.getOwnerId());
        assertEquals(now, state.getCreatedAt());
        assertEquals(Set.of(ownerId), state.getMembers());
        assertTrue(state.isMember(ownerId));
    }

    @Test
    void addMemberAddsUserToServer() {
        ServerState state = givenCreatedServer();
        long now = System.currentTimeMillis();
        var event = new ServerEntityProtocol.MemberAddedEvent(serverId, userId, now);

        state = state.applyEvent(event);

        assertEquals(Set.of(ownerId, userId), state.getMembers());
        assertTrue(state.isMember(userId));
    }

    @Test
    void addExistingMemberIsIdempotent() {
        ServerState state = givenCreatedServer();
        long now = System.currentTimeMillis();
        var event = new ServerEntityProtocol.MemberAddedEvent(serverId, ownerId, now);

        state = state.applyEvent(event);

        assertEquals(Set.of(ownerId), state.getMembers());
    }

    @Test
    void removeMemberRemovesUserFromServer() {
        ServerState state = givenCreatedServer();
        state = state.applyEvent(new ServerEntityProtocol.MemberAddedEvent(serverId, userId, System.currentTimeMillis()));
        var event = new ServerEntityProtocol.MemberRemovedEvent(serverId, userId, System.currentTimeMillis());

        state = state.applyEvent(event);

        assertEquals(Set.of(ownerId), state.getMembers());
        assertFalse(state.isMember(userId));
    }

    @Test
    void removeNonMemberDoesNothing() {
        ServerState state = givenCreatedServer();
        var event = new ServerEntityProtocol.MemberRemovedEvent(serverId, userId, System.currentTimeMillis());

        state = state.applyEvent(event);

        assertEquals(Set.of(ownerId), state.getMembers());
    }

    @Test
    void createChannelAddsChannel() {
        ServerState state = givenCreatedServer();
        UUID channelId = UUID.randomUUID();
        String channelName = "general";
        var event = new ServerEntityProtocol.ChannelCreatedEvent(serverId, channelId, channelName, ownerId, System.currentTimeMillis());

        state = state.applyEvent(event);

        assertEquals(Map.of(channelId, channelName), state.getChannels());
    }

    @Test
    void createMultipleChannels() {
        ServerState state = givenCreatedServer();
        UUID c1 = UUID.randomUUID(), c2 = UUID.randomUUID();
        state = state.applyEvent(new ServerEntityProtocol.ChannelCreatedEvent(serverId, c1, "general", ownerId, 1));
        state = state.applyEvent(new ServerEntityProtocol.ChannelCreatedEvent(serverId, c2, "random", ownerId, 2));

        assertEquals(2, state.getChannels().size());
        assertEquals("general", state.getChannels().get(c1));
        assertEquals("random", state.getChannels().get(c2));
    }

    private ServerState givenCreatedServer() {
        ServerState state = ServerState.empty(serverId);
        return state.applyEvent(
                new ServerEntityProtocol.ServerCreatedEvent(serverId, serverName, ownerId, System.currentTimeMillis()));
    }
}
