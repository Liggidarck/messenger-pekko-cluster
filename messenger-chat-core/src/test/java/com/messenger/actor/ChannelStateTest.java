package com.messenger.actor;

import com.messenger.messages.core.ChannelEntityProtocol;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ChannelStateTest {

    private final UUID channelId = UUID.randomUUID();
    private final UUID serverId = UUID.randomUUID();
    private final UUID userId = UUID.randomUUID();
    private final String title = "test-channel";

    @Test
    void emptyStateIsNotCreated() {
        ChannelState state = ChannelState.empty(channelId);
        assertFalse(state.isCreated());
        assertTrue(state.isDirect());
        assertEquals(0, state.getMessageCount());
        assertNull(state.getServerId());
    }

    @Test
    void createServerChannel() {
        ChannelState state = ChannelState.empty(channelId);
        long now = System.currentTimeMillis();
        var event = new ChannelEntityProtocol.ChannelCreatedEvent(
                serverId, channelId, null, title, userId, now);

        state = state.applyEvent(event);

        assertTrue(state.isCreated());
        assertFalse(state.isDirect());
        assertEquals(serverId, state.getServerId());
        assertEquals(channelId, state.getChannelId());
        assertEquals(title, state.getTitle());
        assertEquals(userId, state.getCreatedBy());
        assertEquals(now, state.getCreatedAt());
    }

    @Test
    void createDirectChannel() {
        ChannelState state = ChannelState.empty(channelId);
        Set<UUID> members = Set.of(UUID.randomUUID(), UUID.randomUUID());
        var event = new ChannelEntityProtocol.ChannelCreatedEvent(
                null, channelId, members, "DM", userId, System.currentTimeMillis());

        state = state.applyEvent(event);

        assertTrue(state.isCreated());
        assertTrue(state.isDirect());
        assertNull(state.getServerId());
        assertEquals(members, state.getDirectMembers());
    }

    @Test
    void messageSentIncrementsCount() {
        ChannelState state = givenCreatedChannel();
        int before = state.getMessageCount();

        state = state.applyEvent(new ChannelEntityProtocol.MessageSentEvent(
                channelId, UUID.randomUUID(), userId, "hello", List.of(), null, 1));

        assertEquals(before + 1, state.getMessageCount());
    }

    @Test
    void editMessageDoesNotChangeCount() {
        ChannelState state = givenCreatedChannel();
        state = state.applyEvent(new ChannelEntityProtocol.MessageSentEvent(
                channelId, UUID.randomUUID(), userId, "hello", List.of(), null, 1));
        int before = state.getMessageCount();

        state = state.applyEvent(new ChannelEntityProtocol.EditMessageEvent(
                channelId, UUID.randomUUID(), userId, "edited", 2));

        assertEquals(before, state.getMessageCount());
    }

    @Test
    void deleteMessageDoesNotChangeCount() {
        ChannelState state = givenCreatedChannel();
        state = state.applyEvent(new ChannelEntityProtocol.MessageSentEvent(
                channelId, UUID.randomUUID(), userId, "hello", List.of(), null, 1));
        int before = state.getMessageCount();

        state = state.applyEvent(new ChannelEntityProtocol.DeleteMessageEvent(
                channelId, UUID.randomUUID(), userId));

        assertEquals(before, state.getMessageCount());
    }

    @Test
    void directMembersAreImmutable() {
        ChannelState state = givenCreatedChannel();
        assertThrows(UnsupportedOperationException.class, () -> state.getDirectMembers().add(UUID.randomUUID()));
    }

    private ChannelState givenCreatedChannel() {
        ChannelState state = ChannelState.empty(channelId);
        return state.applyEvent(
                new ChannelEntityProtocol.ChannelCreatedEvent(
                        serverId, channelId, null, title, userId, System.currentTimeMillis()));
    }
}
