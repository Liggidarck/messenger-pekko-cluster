package com.messenger.http;

import com.messenger.messages.auth.UserCrudProtocol;
import com.messenger.messages.core.ServerEntityProtocol;
import com.messenger.messages.core.UserEntityProtocol;
import com.messenger.requests.AddMemberRequest;
import com.messenger.requests.CreateChannelRequest;
import com.messenger.requests.CreateServerRequest;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.http.javadsl.marshallers.jackson.Jackson;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.server.Route;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static org.apache.pekko.http.javadsl.server.PathMatchers.uuidSegment;

public class ServerRouter extends BaseRouter {

    private final ClusterSharding sharding;
    private final ActorRef<UserCrudProtocol.UserCrudCommand> unitsUserActor;

    public ServerRouter(ActorSystem<?> actorSystem,
                        ActorRef<com.messenger.messages.auth.AuthValidatorMessages.ValidateToken> validatorActor,
                        ClusterSharding sharding,
                        ActorRef<UserCrudProtocol.UserCrudCommand> unitsUserActor) {
        super(actorSystem, validatorActor);
        this.sharding = sharding;
        this.unitsUserActor = unitsUserActor;
    }

    @Override
    public Route createRoutes() {
        return pathPrefix("servers", () ->
                concat(
                        pathEnd(() ->
                                concat(
                                        get(() -> authenticated(this::listServers)),
                                        post(() ->
                                                entity(Jackson.unmarshaller(CreateServerRequest.class), request ->
                                                        authenticated(user -> createServer(user, request))
                                                )
                                        )
                                )
                        ),
                        pathPrefix(uuidSegment(), serverId ->
                                concat(
                                        pathEnd(() ->
                                                get(() -> authenticated(user -> getServerProfile(user, serverId)))
                                        ),
                                        path("members", () ->
                                                concat(
                                                        get(() -> authenticated(user -> getMembers(user, serverId))),
                                                        post(() ->
                                                                entity(Jackson.unmarshaller(AddMemberRequest.class), request ->
                                                                        authenticated(user -> addMember(user, serverId, request))
                                                                )
                                                        )
                                                )
                                        ),
                                        path("channels", () ->
                                                concat(
                                                        get(() -> authenticated(user -> getChannels(user, serverId))),
                                                        post(() ->
                                                                entity(Jackson.unmarshaller(CreateChannelRequest.class), request ->
                                                                        authenticated(user -> createChannel(user, serverId, request))
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                )
        );
    }

    private Route listServers(User user) {
        EntityRef<UserEntityProtocol.Command> userEntity = sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, user.userId.toString());

        CompletionStage<UserEntityProtocol.GetServersResponse> result = AskPattern.ask(
                userEntity,
                UserEntityProtocol.GetServers::new,
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            var json = response.servers().stream().map(s -> Map.of(
                    "serverId", s.serverId().toString(),
                    "serverName", s.serverName(),
                    "ownerId", s.ownerId() != null ? s.ownerId().toString() : "",
                    "createdAt", s.createdAt()
            )).toList();
            return complete(StatusCodes.OK, Map.of("servers", json), Jackson.marshaller());
        });
    }

    private Route getServerProfile(User user, UUID serverId) {
        EntityRef<ServerEntityProtocol.Command> entityRef = sharding.entityRefFor(ServerEntityProtocol.ENTITY_KEY, serverId.toString());

        CompletionStage<ServerEntityProtocol.Response> result = AskPattern.ask(
                entityRef,
                replyTo -> new ServerEntityProtocol.GetServerProfile(replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            if (response instanceof ServerEntityProtocol.ServerProfile p) {
                return complete(StatusCodes.OK, Map.of(
                        "serverId", p.serverId().toString(),
                        "name", p.name(),
                        "ownerId", p.ownerId(),
                        "createdAt", p.createdAt(),
                        "memberCount", p.members().size(),
                        "channelCount", p.channels().size()
                ), Jackson.marshaller());
            }
            return completeHttp(new com.messenger.messages.HttpFinalResponse(404,
                    mapper.createObjectNode().put("error", "Server not found")));
        });
    }

    private Route getMembers(User user, UUID serverId) {
        EntityRef<ServerEntityProtocol.Command> entityRef = sharding.entityRefFor(ServerEntityProtocol.ENTITY_KEY, serverId.toString());

        CompletionStage<ServerEntityProtocol.Response> result = AskPattern.ask(
                entityRef,
                replyTo -> new ServerEntityProtocol.GetMembers(replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            if (response instanceof ServerEntityProtocol.Members res) {
                return complete(StatusCodes.OK, res.userIds(), Jackson.marshaller());
            } else {
                return completeHttp(new com.messenger.messages.HttpFinalResponse(500,
                        mapper.createObjectNode().put("error", "Internal server error")));
            }
        });
    }

    private Route getChannels(User user, UUID serverId) {
        EntityRef<ServerEntityProtocol.Command> entityRef = sharding.entityRefFor(ServerEntityProtocol.ENTITY_KEY, serverId.toString());

        CompletionStage<ServerEntityProtocol.Response> result = AskPattern.ask(
                entityRef,
                replyTo -> new ServerEntityProtocol.GetChannels(replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            if (response instanceof ServerEntityProtocol.Channels c) {
                var channels = c.channels().entrySet().stream()
                        .map(e -> Map.of("channelId", e.getKey().toString(), "channelName", e.getValue()))
                        .toList();
                return complete(StatusCodes.OK, Map.of("serverId", serverId.toString(), "channels", channels), Jackson.marshaller());
            }
            return completeHttp(new com.messenger.messages.HttpFinalResponse(500,
                    mapper.createObjectNode().put("error", "Internal server error")));
        });
    }

    private Route createServer(User user, CreateServerRequest request) {
        UUID serverId = UUID.randomUUID();
        EntityRef<ServerEntityProtocol.Command> entityRef = sharding.entityRefFor(ServerEntityProtocol.ENTITY_KEY, serverId.toString());

        CompletionStage<ServerEntityProtocol.Response> result = AskPattern.ask(
                entityRef,
                replyTo -> new ServerEntityProtocol.CreateServer(request.name(), user.userId, replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            if (response instanceof ServerEntityProtocol.ServerCreated res) {
                return completeHttp(new com.messenger.messages.HttpFinalResponse(201,
                        mapper.createObjectNode().put("serverId", res.serverId().toString())));
            } else {
                return completeHttp(new com.messenger.messages.HttpFinalResponse(500, mapper.createObjectNode().put("error", "Internal server error")));
            }
        });
    }

    private Route addMember(User user, UUID serverId, AddMemberRequest request) {
        if (request.email() != null && !request.email().isBlank()) {
            CompletionStage<UserCrudProtocol.UserResponse> userLookup = AskPattern.ask(
                    unitsUserActor,
                    replyTo -> new UserCrudProtocol.GetUser_Crud_Email(request.email(), replyTo),
                    Duration.ofSeconds(3),
                    system.scheduler()
            );

            return onSuccess(userLookup, response -> {
                if (response instanceof UserCrudProtocol.UserFound found) {
                    return proceedToAddMember(serverId, found.user().id());
                } else {
                    return completeHttp(new com.messenger.messages.HttpFinalResponse(404,
                            mapper.createObjectNode().put("error", "User with this email not found")));
                }
            });
        } else if (request.userId() != null) {
            return proceedToAddMember(serverId, request.userId());
        } else {
            return completeHttp(new com.messenger.messages.HttpFinalResponse(400,
                    mapper.createObjectNode().put("error", "Either userId or email must be provided")));
        }
    }

    private Route proceedToAddMember(UUID serverId, UUID userId) {
        EntityRef<ServerEntityProtocol.Command> entityRef = sharding.entityRefFor(ServerEntityProtocol.ENTITY_KEY, serverId.toString());

        CompletionStage<ServerEntityProtocol.Response> result = AskPattern.ask(
                entityRef,
                replyTo -> new ServerEntityProtocol.AddMember(userId, replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            if (response instanceof ServerEntityProtocol.MemberAdded) {
                return completeHttp(new com.messenger.messages.HttpFinalResponse(200,
                        mapper.createObjectNode().put("status", "success")));
            } else if (response instanceof ServerEntityProtocol.ErrorResponse err) {
                return completeHttp(new com.messenger.messages.HttpFinalResponse(400,
                        mapper.createObjectNode().put("error", err.message())));
            } else {
                return completeHttp(new com.messenger.messages.HttpFinalResponse(500,
                        mapper.createObjectNode().put("error", "Internal server error")));
            }
        });
    }

    private Route createChannel(User user, UUID serverId, CreateChannelRequest request) {
        EntityRef<ServerEntityProtocol.Command> entityRef = sharding.entityRefFor(ServerEntityProtocol.ENTITY_KEY, serverId.toString());

        CompletionStage<ServerEntityProtocol.Response> result = AskPattern.ask(
                entityRef,
                replyTo -> new ServerEntityProtocol.CreateChannel(request.name(), replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        );

        return onSuccess(result, response -> {
            if (response instanceof ServerEntityProtocol.ChannelCreated res) {
                return complete(StatusCodes.OK, Map.of(
                        "serverId", serverId.toString(),
                        "channelId", res.channelId().toString(),
                        "channelName", request.name()
                ), Jackson.marshaller());
            } else {
                return completeHttp(new com.messenger.messages.HttpFinalResponse(500, mapper.createObjectNode().put("error", "Internal server error")));
            }
        });
    }

}
