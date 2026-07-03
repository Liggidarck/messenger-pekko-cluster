package com.messenger.messages.core;

import com.messenger.messages.CborSerializable;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class ChannelEntityProtocol {

    public static final EntityTypeKey<Command> ENTITY_KEY = EntityTypeKey.create(Command.class, "ChannelEntity");

    public interface Command extends CborSerializable {}
    public interface Response extends CborSerializable {}
    public interface Event extends CborSerializable {}

    public record CreateChannel(UUID serverId, Set<UUID> directMembers, String title, UUID createdBy, ActorRef<Response> replyTo) implements Command {}
    public record SendMessage(UUID channelId, UUID senderId, String text, List<String> mediaUrls, UUID replyToMessageId, ActorRef<Response> replyTo) implements Command {}
    public record EditMessage(UUID channelId, UUID messageId, UUID senderId, String text, ActorRef<Response> replyTo) implements Command {}
    public record DeleteMessage(UUID channelId, UUID messageId, UUID senderId, ActorRef<Response> replyTo) implements Command {}
    public record GetHistory(int limit, UUID beforeMessageId, ActorRef<Response> replyTo) implements Command {}
    public record GetHistoryResult(List<MessageDto> messages, boolean hasMore, ActorRef<Response> replyTo) implements Command {}
    public record Typing(UUID userId, ActorRef<Response> replyTo) implements Command {}

    public record ChannelCreated(UUID channelId) implements Response {}
    public record MessageSent(UUID messageId, long createdAt) implements Response {}
    public record MessageEdited(UUID messageId, long editedAt) implements Response {}
    public record MessageDeleted(UUID messageId) implements Response {}
    public record HistoryResponse(List<MessageDto> messages, boolean hasMore) implements Response {}
    public record ErrorResponse(String code, String message) implements Response {}

    public record ChannelCreatedEvent(UUID serverId, UUID channelId, Set<UUID> directMembers, String title, UUID createdBy, long createdAt) implements Event {}
    public record MessageSentEvent(UUID channelId, UUID messageId, UUID senderId, String text, List<String> mediaUrls, UUID replyToMessageId, long createdAt) implements Event {}
    public record EditMessageEvent(UUID channelId, UUID messageId, UUID senderId, String text, long editedAt) implements Event {}
    public record DeleteMessageEvent(UUID channelId, UUID messageId, UUID senderId) implements Event {}

    public record MessageDto(UUID messageId, UUID senderId, String text, long createdAt,
                             List<String> mediaUrls, UUID replyToMessageId,
                             boolean isEdited, Long editedAt, boolean isDeleted) implements CborSerializable {}
}
