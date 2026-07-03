package com.messenger.actor;

import com.messenger.messages.core.ChannelEntityProtocol;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

public class ChannelState {

    private final UUID channelId;
    private final UUID serverId;
    private final Set<UUID> directMembers;
    private final String title;
    private final long createdAt;
    private final UUID createdBy;
    private int messageCount;

    public ChannelState(UUID channelId, UUID serverId, Set<UUID> directMembers, String title, long createdAt, UUID createdBy, int messageCount) {
        this.channelId = channelId;
        this.serverId = serverId;
        this.directMembers = directMembers == null ? Collections.emptySet() : Set.copyOf(directMembers);
        this.title = title;
        this.createdAt = createdAt;
        this.createdBy = createdBy;
        this.messageCount = messageCount;
    }

    public static ChannelState empty(UUID channelId) {
        return new ChannelState(channelId, null, Collections.emptySet(), null, 0, null, 0);
    }

    public ChannelState applyEvent(ChannelEntityProtocol.Event event) {
        if (event instanceof ChannelEntityProtocol.ChannelCreatedEvent e) {
            return new ChannelState(e.channelId(), e.serverId(), e.directMembers(), e.title(), e.createdAt(), e.createdBy(), 0);
        }
        if (event instanceof ChannelEntityProtocol.MessageSentEvent) {
            messageCount++;
            return this;
        }
        if (event instanceof ChannelEntityProtocol.EditMessageEvent || event instanceof ChannelEntityProtocol.DeleteMessageEvent) {
            return this;
        }
        return this;
    }

    public boolean isCreated() {
        return createdAt > 0;
    }

    public boolean isDirect() {
        return serverId == null;
    }

    public Set<UUID> getDirectMembers() { return directMembers; }
    public UUID getChannelId() { return channelId; }
    public UUID getServerId() { return serverId; }
    public String getTitle() { return title; }
    public long getCreatedAt() { return createdAt; }
    public UUID getCreatedBy() { return createdBy; }
    public int getMessageCount() { return messageCount; }
}
