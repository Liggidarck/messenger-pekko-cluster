package com.messenger.http;

import com.messenger.messages.auth.AuthValidatorMessages;
import com.messenger.messages.core.ChannelEntityProtocol;
import com.messenger.messages.core.UserEntityProtocol;
import com.messenger.requests.EditMessageRequest;
import com.messenger.requests.SendMessageRequest;
import com.messenger.requests.UpdateReadStateRequest;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.http.javadsl.marshallers.jackson.Jackson;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.server.Route;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;

import static org.apache.pekko.http.javadsl.server.PathMatchers.uuidSegment;

public class MessageRouter extends BaseRouter {

    private final ClusterSharding sharding;

    public MessageRouter(ActorSystem<?> actorSystem,
                         ActorRef<AuthValidatorMessages.ValidateToken> validatorActor,
                         ClusterSharding sharding) {
        super(actorSystem, validatorActor);
        this.sharding = sharding;
    }

    @Override
    public Route createRoutes() {
        return pathPrefix("channels", () ->
                pathPrefix(uuidSegment(), channelId ->
                        concat(
                                path("messages", () ->
                                        concat(
                                                get(() -> parameterOptional("limit", limitOpt -> parameterOptional("beforeMessageId", beforeOpt ->
                                                        authenticated(user -> getHistory(user, channelId, limitOpt, beforeOpt))
                                                ))),
                                                post(() ->
                                                        entity(Jackson.unmarshaller(SendMessageRequest.class), request ->
                                                                authenticated(user -> sendMessage(user, channelId, request))
                                                        )
                                                )
                                        )
                                ),
                                pathPrefix("messages", () ->
                                        path(uuidSegment(), messageId ->
                                                concat(
                                                        put(() ->
                                                                entity(Jackson.unmarshaller(EditMessageRequest.class), request ->
                                                                        authenticated(user -> editMessage(user, channelId, messageId, request))
                                                                )
                                                        ),
                                                        delete(() ->
                                                                authenticated(user -> deleteMessage(user, channelId, messageId))
                                                        )
                                                )
                                        )
                                ),
                                path("read-state", () ->
                                        concat(
                                                get(() -> authenticated(user -> getReadState(user, channelId))),
                                                put(() ->
                                                        entity(Jackson.unmarshaller(UpdateReadStateRequest.class), request ->
                                                                authenticated(user -> updateReadState(user, channelId, request))
                                                        )
                                                )
                                        )
                                )
                        )
                )
        );
    }

    private Route getHistory(User user, UUID channelId, Optional<String> limitOpt, Optional<String> beforeOpt) {
        int limit = 50;
        if (limitOpt.isPresent()) {
            try { limit = Integer.parseInt(limitOpt.get()); } catch (NumberFormatException ignored) {}
        }
        final int finalLimit = limit;
        UUID tmpBefore = null;
        if (beforeOpt.isPresent()) {
            try { tmpBefore = UUID.fromString(beforeOpt.get()); } catch (IllegalArgumentException ignored) {}
        }
        final UUID beforeMessageId = tmpBefore;

        EntityRef<ChannelEntityProtocol.Command> channelEntity = sharding.entityRefFor(
                ChannelEntityProtocol.ENTITY_KEY, channelId.toString());

        CompletionStage<ChannelEntityProtocol.Response> result = AskPattern.ask(
                channelEntity,
                ref -> new ChannelEntityProtocol.GetHistory(finalLimit, beforeMessageId, ref),
                Duration.ofSeconds(10),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            if (response instanceof ChannelEntityProtocol.HistoryResponse h) {
                List<Map<String, Object>> messages = h.messages().stream().map(m -> {
                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put("messageId", m.messageId().toString());
                    map.put("senderId", m.senderId());
                    map.put("text", m.text());
                    map.put("createdAt", m.createdAt());
                    map.put("mediaUrls", m.mediaUrls());
                    map.put("replyToMessageId", m.replyToMessageId() != null ? m.replyToMessageId().toString() : null);
                    map.put("isEdited", m.isEdited());
                    map.put("editedAt", m.editedAt());
                    map.put("isDeleted", m.isDeleted());
                    return map;
                }).toList();
                return complete(StatusCodes.OK, Map.of(
                        "channelId", channelId.toString(),
                        "messages", messages,
                        "hasMore", h.hasMore()
                ), Jackson.marshaller());
            }
            return completeHttp(new com.messenger.messages.HttpFinalResponse(500,
                    mapper.createObjectNode().put("error", "Failed to fetch history")));
        });
    }

    private Route sendMessage(User user, UUID channelId, SendMessageRequest request) {
        EntityRef<ChannelEntityProtocol.Command> entity = sharding.entityRefFor(
                ChannelEntityProtocol.ENTITY_KEY, channelId.toString());

        CompletionStage<ChannelEntityProtocol.Response> result = AskPattern.ask(
                entity,
                ref -> new ChannelEntityProtocol.SendMessage(
                        channelId, user.userId, request.text(),
                        request.mediaUrls() != null ? request.mediaUrls() : List.of(),
                        request.replyToMessageId(), ref),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            if (response instanceof ChannelEntityProtocol.MessageSent s) {
                return complete(StatusCodes.CREATED, Map.of(
                        "messageId", s.messageId().toString(),
                        "createdAt", s.createdAt()
                ), Jackson.marshaller());
            }
            return completeHttp(new com.messenger.messages.HttpFinalResponse(400,
                    mapper.createObjectNode().put("error", "Failed to send message")));
        });
    }

    private Route editMessage(User user, UUID channelId, UUID messageId, EditMessageRequest request) {
        EntityRef<ChannelEntityProtocol.Command> entity = sharding.entityRefFor(
                ChannelEntityProtocol.ENTITY_KEY, channelId.toString());

        CompletionStage<ChannelEntityProtocol.Response> result = AskPattern.ask(
                entity,
                ref -> new ChannelEntityProtocol.EditMessage(
                        channelId, messageId, user.userId, request.text(), ref),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            if (response instanceof ChannelEntityProtocol.MessageEdited e) {
                return complete(StatusCodes.OK, Map.of(
                        "messageId", e.messageId().toString(),
                        "editedAt", e.editedAt()
                ), Jackson.marshaller());
            }
            return completeHttp(new com.messenger.messages.HttpFinalResponse(400,
                    mapper.createObjectNode().put("error", "Failed to edit message")));
        });
    }

    private Route deleteMessage(User user, UUID channelId, UUID messageId) {
        EntityRef<ChannelEntityProtocol.Command> entity = sharding.entityRefFor(
                ChannelEntityProtocol.ENTITY_KEY, channelId.toString());

        CompletionStage<ChannelEntityProtocol.Response> result = AskPattern.ask(
                entity,
                ref -> new ChannelEntityProtocol.DeleteMessage(
                        channelId, messageId, user.userId, ref),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            if (response instanceof ChannelEntityProtocol.MessageDeleted) {
                return complete(StatusCodes.OK, Map.of("status", "deleted"), Jackson.marshaller());
            }
            return completeHttp(new com.messenger.messages.HttpFinalResponse(400,
                    mapper.createObjectNode().put("error", "Failed to delete message")));
        });
    }

    private Route getReadState(User user, UUID channelId) {
        EntityRef<UserEntityProtocol.Command> userEntity = sharding.entityRefFor(
                UserEntityProtocol.ENTITY_KEY, user.userId.toString());

        CompletionStage<UserEntityProtocol.ReadStateResponse> result = AskPattern.ask(
                userEntity,
                ref -> new UserEntityProtocol.GetReadState(channelId, ref),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("channelId", response.channelId().toString());
            if (response.lastReadMessageId() != null) {
                resp.put("lastReadMessageId", response.lastReadMessageId().toString());
            }
            resp.put("unreadCount", response.unreadCount());
            return complete(StatusCodes.OK, resp, Jackson.marshaller());
        });
    }

    private Route updateReadState(User user, UUID channelId, UpdateReadStateRequest request) {
        EntityRef<UserEntityProtocol.Command> userEntity = sharding.entityRefFor(
                UserEntityProtocol.ENTITY_KEY, user.userId.toString());

        CompletionStage<UserEntityProtocol.ReadStateResponse> result = AskPattern.ask(
                userEntity,
                ref -> new UserEntityProtocol.UpdateReadState(
                        channelId, request.lastReadMessageId(), ref),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response ->
                complete(StatusCodes.OK, Map.of(
                        "channelId", response.channelId().toString(),
                        "lastReadMessageId", response.lastReadMessageId() != null ? response.lastReadMessageId().toString() : null,
                        "unreadCount", response.unreadCount()
                ), Jackson.marshaller())
        );
    }

}
