package com.messenger.actor;

import com.messenger.messages.core.ServerEntityProtocol;

import java.util.*;

public class ServerState {

    private final UUID serverId;
    private final String serverName;
    private final UUID ownerId;
    private final Set<UUID> members;
    private final Map<UUID, String> channels;
    private final long createdAt;

    public ServerState(UUID serverId, String serverName, UUID ownerId, Set<UUID> members, Map<UUID, String> channels, long createdAt) {
        this.serverId = serverId;
        this.serverName = serverName;
        this.ownerId = ownerId;
        this.members = new HashSet<>(members);
        this.channels = new HashMap<>(channels);
        this.createdAt = createdAt;
    }

    public static ServerState empty(UUID serverId) {
        return new ServerState(serverId, null, null, Set.of(), Map.of(), 0);
    }

    public ServerState applyEvent(ServerEntityProtocol.Event event) {
        if (event instanceof ServerEntityProtocol.ServerCreatedEvent e) {
            var newMembers = new HashSet<UUID>();
            newMembers.add(e.ownerId());
            return new ServerState(e.serverId(), e.serverName(), e.ownerId(), newMembers, Map.of(), e.createdAt());
        }
        if (event instanceof ServerEntityProtocol.MemberAddedEvent e) {
            var newMembers = new HashSet<>(members);
            newMembers.add(e.userId());
            return new ServerState(serverId, serverName, ownerId, newMembers, channels, createdAt);
        }
        if (event instanceof ServerEntityProtocol.MemberRemovedEvent e) {
            var newMembers = new HashSet<>(members);
            newMembers.remove(e.userId());
            return new ServerState(serverId, serverName, ownerId, newMembers, channels, createdAt);
        }
        if (event instanceof ServerEntityProtocol.ChannelCreatedEvent e) {
            var newChannels = new HashMap<>(channels);
            newChannels.put(e.channelId(), e.channelName());
            return new ServerState(serverId, serverName, ownerId, members, newChannels, createdAt);
        }
        return this;
    }

    public boolean isCreated() {
        return createdAt > 0;
    }

    public boolean isMember(UUID userId) {
        return members.contains(userId);
    }

    public UUID getServerId() { return serverId; }
    public String getServerName() { return serverName; }
    public UUID getOwnerId() { return ownerId; }
    public Set<UUID> getMembers() { return Collections.unmodifiableSet(members); }
    public Map<UUID, String> getChannels() { return Collections.unmodifiableMap(channels); }
    public long getCreatedAt() { return createdAt; }
}
