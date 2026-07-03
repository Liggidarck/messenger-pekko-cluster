package com.messenger.messages.core;

import com.messenger.messages.CborSerializable;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ServerEntityProtocol {
    public static final EntityTypeKey<Command> ENTITY_KEY = EntityTypeKey.create(Command.class, "ServerEntity");

    public interface Command extends CborSerializable {}
    public interface Response extends CborSerializable {}
    public interface Event extends CborSerializable {}

    public record CreateServer(String serverName, UUID ownerId, ActorRef<Response> replyTo) implements Command {}
    public record AddMember(UUID userId, ActorRef<Response> replyTo) implements Command {}
    public record RemoveMember(UUID userId, ActorRef<Response> replyTo) implements Command {}
    public record CreateChannel(String channelName, ActorRef<Response> replyTo) implements Command {}
    public record GetChannels(ActorRef<Response> replyTo) implements Command {}
    public record GetMembers(ActorRef<Response> replyTo) implements Command {}
    public record GetServerProfile(ActorRef<Response> replyTo) implements Command {}
    public record BroadcastChannelMessage(UUID channelId, UUID messageId, UUID senderId, String text, long createdAt, List<String> mediaUrls, UUID replyToMessageId) implements Command {}
    public record BroadcastEditMessage(UUID channelId, UUID messageId, String text, long editedAt) implements Command {}
    public record BroadcastDeleteMessage(UUID channelId, UUID messageId) implements Command {}
    public record BroadcastTyping(UUID channelId, UUID userId) implements Command {}

    public record ServerCreated(UUID serverId) implements Response {}
    public record MemberAdded(UUID serverId, UUID userId) implements Response {}
    public record MemberRemoved(UUID serverId, UUID userId) implements Response {}
    public record ChannelCreated(UUID channelId) implements Response {}
    public record Channels(Map<UUID, String> channels) implements Response {}
    public record Members(Set<UUID> userIds) implements Response {}
    public record ServerProfile(UUID serverId, String name, UUID ownerId, long createdAt, Set<UUID> members, Map<UUID, String> channels) implements Response {}
    public record ErrorResponse(String code, String message) implements Response {}

    public record ServerCreatedEvent(UUID serverId, String serverName, UUID ownerId, long createdAt) implements Event {}
    public record MemberAddedEvent(UUID serverId, UUID userId, long createdAt) implements Event {}
    public record MemberRemovedEvent(UUID serverId, UUID userId, long createdAt) implements Event {}
    public record ChannelCreatedEvent(UUID serverId, UUID channelId, String channelName, UUID createdBy, long createdAt) implements Event {}
}
