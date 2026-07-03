package com.messenger.messages.core;

import com.messenger.messages.CborSerializable;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

import java.util.List;
import java.util.UUID;

public class UserEntityProtocol {

    public static final EntityTypeKey<Command> ENTITY_KEY = EntityTypeKey.create(Command.class, "UserEntity");

    public interface Command extends CborSerializable {}

    public record ServerDto(UUID serverId, String serverName, UUID ownerId, long createdAt) implements CborSerializable {}
    public record GetServersResponse(List<ServerDto> servers) implements CborSerializable {}
    public record OutgoingMessage(String payload) implements CborSerializable {}

    public record DeviceConnected(String sessionId, ActorRef<OutgoingMessage> replyTo) implements Command {}
    public record DeviceDisconnected(String sessionId) implements Command {}
    public record NotifyNewChannelMessage(UUID serverId, UUID channelId, UUID messageId, UUID fromUserId, String text, long createdAt, List<String> mediaUrls, UUID replyToMessageId) implements Command {}
    public record DeliverMessage(UUID fromUserId, String text) implements Command {}
    public record NotifyServerCreated(UUID serverId, String serverName) implements Command {}
    public record NotifyChannelCreated(UUID serverId, UUID channelId, String channelName) implements Command {}
    public record GetServers(ActorRef<GetServersResponse> replyTo) implements Command {}
    public record NotifyTyping(UUID channelId, UUID userId) implements Command {}
    public record NotifyEditMessage(UUID serverId, UUID channelId, UUID messageId, String text, long editedAt) implements Command {}
    public record NotifyDeleteMessage(UUID serverId, UUID channelId, UUID messageId) implements Command {}

    public record GetReadState(UUID channelId, ActorRef<ReadStateResponse> replyTo) implements Command {}
    public record UpdateReadState(UUID channelId, UUID lastReadMessageId, ActorRef<ReadStateResponse> replyTo) implements Command {}
    public record ReadStateResponse(UUID channelId, UUID lastReadMessageId, int unreadCount) implements CborSerializable {}

}
