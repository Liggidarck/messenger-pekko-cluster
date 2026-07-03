package com.messenger.actor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.messenger.cassandra.CassandraSessionHolder;
import com.messenger.messages.core.ChannelEntityProtocol;
import com.messenger.messages.core.ServerEntityProtocol;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityContext;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.apache.pekko.persistence.typed.javadsl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.messenger.messages.core.UserEntityProtocol;

import java.time.Instant;
import java.util.UUID;


public class ServerEntityActor extends EventSourcedBehavior<ServerEntityProtocol.Command, ServerEntityProtocol.Event, ServerState> {

    private static final Logger log = LoggerFactory.getLogger(ServerEntityActor.class);

    private final ActorContext<ServerEntityProtocol.Command> ctx;
    private final ClusterSharding sharding;
    private final CqlSession cql;
    private final UUID serverId;

    private interface AdaptedResponse extends ServerEntityProtocol.Command {}
    private static class ChannelResponseAdapter implements AdaptedResponse {
        public final ChannelEntityProtocol.Response response;
        public ChannelResponseAdapter(ChannelEntityProtocol.Response response) {
            this.response = response;
        }
    }


    public ServerEntityActor(PersistenceId persistenceId, ActorContext<ServerEntityProtocol.Command> ctx, ClusterSharding sharding, CqlSession cql) {
        super(persistenceId);
        this.ctx = ctx;
        this.sharding = sharding;
        this.cql = cql;
        this.serverId = UUID.fromString(persistenceId.entityId());
    }

    public static Behavior<ServerEntityProtocol.Command> create(EntityContext<ServerEntityProtocol.Command> entityContext) {
        UUID serverId = UUID.fromString(entityContext.getEntityId());
        PersistenceId persistenceId = PersistenceId.of("ServerEntity", serverId.toString());
        return Behaviors.setup(ctx -> {
            ClusterSharding sharding = ClusterSharding.get(ctx.getSystem());
            CqlSession cql = CassandraSessionHolder.getSession();
            return new ServerEntityActor(persistenceId, ctx, sharding, cql);
        });
    }

    @Override
    public ServerState emptyState() {
        return ServerState.empty(serverId);
    }

    @Override
    public CommandHandler<ServerEntityProtocol.Command, ServerEntityProtocol.Event, ServerState> commandHandler() {
        return newCommandHandlerBuilder()
                .forAnyState()
                .onCommand(ServerEntityProtocol.CreateServer.class, this::onCreateServer)
                .onCommand(ServerEntityProtocol.AddMember.class, this::onAddMember)
                .onCommand(ServerEntityProtocol.RemoveMember.class, this::onRemoveMember)
                .onCommand(ServerEntityProtocol.CreateChannel.class, this::onCreateChannel)
                .onCommand(ServerEntityProtocol.GetMembers.class, this::onGetMembers)
                .onCommand(ServerEntityProtocol.GetServerProfile.class, this::onGetServerProfile)
                .onCommand(ServerEntityProtocol.BroadcastChannelMessage.class, this::onBroadcastChannelMessage)
                .onCommand(ServerEntityProtocol.BroadcastEditMessage.class, this::onBroadcastEditMessage)
                .onCommand(ServerEntityProtocol.BroadcastDeleteMessage.class, this::onBroadcastDeleteMessage)
                .onCommand(ServerEntityProtocol.GetChannels.class, this::onGetChannels)
                .onCommand(ChannelResponseAdapter.class, this::onChannelResponse)
                .onCommand(ServerEntityProtocol.BroadcastTyping.class, this::onBroadcastTyping)
                .build();
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onBroadcastTyping(ServerState state, ServerEntityProtocol.BroadcastTyping cmd) {
        for (UUID memberId : state.getMembers()) {
            if (!memberId.equals(cmd.userId())) {
                sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, memberId.toString())
                        .tell(new UserEntityProtocol.NotifyTyping(cmd.channelId(), cmd.userId()));
            }
        }
        return Effect().none();
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onGetServerProfile(ServerState state, ServerEntityProtocol.GetServerProfile cmd) {
        if (!state.isCreated()) {
            cmd.replyTo().tell(new ServerEntityProtocol.ErrorResponse("NOT_FOUND", "Server does not exist"));
            return Effect().none();
        }
        cmd.replyTo().tell(new ServerEntityProtocol.ServerProfile(
                serverId, state.getServerName(), state.getOwnerId(), state.getCreatedAt(),
                state.getMembers(), state.getChannels()));
        return Effect().none();
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onGetMembers(ServerState state, ServerEntityProtocol.GetMembers cmd) {
        if (!state.isCreated()) {
            cmd.replyTo().tell(new ServerEntityProtocol.ErrorResponse("NOT_FOUND", "Server does not exist"));
            return Effect().none();
        }
        cmd.replyTo().tell(new ServerEntityProtocol.Members(state.getMembers()));
        return Effect().none();
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onBroadcastChannelMessage(ServerState state, ServerEntityProtocol.BroadcastChannelMessage cmd) {
        if (state.isCreated()) {
            for (UUID memberId : state.getMembers()) {
                EntityRef<UserEntityProtocol.Command> userEntity = sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, memberId.toString());
                userEntity.tell(new UserEntityProtocol.NotifyNewChannelMessage(
                        serverId, cmd.channelId(), cmd.messageId(), cmd.senderId(), cmd.text(), cmd.createdAt(), cmd.mediaUrls(), cmd.replyToMessageId()));
            }
        }
        return Effect().none();
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onBroadcastEditMessage(ServerState state, ServerEntityProtocol.BroadcastEditMessage cmd) {
        if (state.isCreated()) {
            for (UUID memberId : state.getMembers()) {
                sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, memberId.toString())
                        .tell(new UserEntityProtocol.NotifyEditMessage(
                                serverId, cmd.channelId(), cmd.messageId(), cmd.text(), cmd.editedAt()));
            }
        }
        return Effect().none();
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onBroadcastDeleteMessage(ServerState state, ServerEntityProtocol.BroadcastDeleteMessage cmd) {
        if (state.isCreated()) {
            for (UUID memberId : state.getMembers()) {
                sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, memberId.toString())
                        .tell(new UserEntityProtocol.NotifyDeleteMessage(
                                serverId, cmd.channelId(), cmd.messageId()));
            }
        }
        return Effect().none();
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onGetChannels(ServerState state, ServerEntityProtocol.GetChannels cmd) {
        if (!state.isCreated()) {
            cmd.replyTo().tell(new ServerEntityProtocol.ErrorResponse("NOT_FOUND", "Server does not exist"));
            return Effect().none();
        }
        cmd.replyTo().tell(new ServerEntityProtocol.Channels(state.getChannels()));
        return Effect().none();
    }

    @Override
    public EventHandler<ServerState, ServerEntityProtocol.Event> eventHandler() {
        return (state, evt) -> state.applyEvent(evt);
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onCreateServer(ServerState state, ServerEntityProtocol.CreateServer cmd) {
        if (state.isCreated()) {
            cmd.replyTo().tell(new ServerEntityProtocol.ErrorResponse("ALREADY_EXISTS", "Server already exists"));
            return Effect().none();
        }

        long now = Instant.now().toEpochMilli();
        var event = new ServerEntityProtocol.ServerCreatedEvent(serverId, cmd.serverName(), cmd.ownerId(), now);

        return Effect().persist(event).thenRun(() -> {
            log.info("Server [{}] created", serverId);
            cmd.replyTo().tell(new ServerEntityProtocol.ServerCreated(serverId));

            Instant ts = Instant.ofEpochMilli(now);
            cql.executeAsync(
                    "INSERT INTO " + CassandraSessionHolder.KEYSPACE + ".servers " +
                            "(server_id, name, owner_id, description, created_at) " +
                            "VALUES (?, ?, ?, '', ?)",
                    serverId, cmd.serverName(), cmd.ownerId(), ts
            ).whenComplete((r, err) -> {
                if (err != null) log.error("Failed to insert server [{}] into Cassandra", serverId, err);
            });
            cql.executeAsync(
                    "INSERT INTO " + CassandraSessionHolder.KEYSPACE + ".servers_by_user (user_id, server_id, joined_at) VALUES (?, ?, ?)",
                    cmd.ownerId(), serverId, ts
            ).whenComplete((r, err) -> {
                if (err != null) log.error("Failed to insert servers_by_user for owner [{}]", cmd.ownerId(), err);
            });

            EntityRef<UserEntityProtocol.Command> userEntity = sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, cmd.ownerId().toString());
            userEntity.tell(new UserEntityProtocol.NotifyServerCreated(serverId, cmd.serverName()));
        });
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onAddMember(ServerState state, ServerEntityProtocol.AddMember cmd) {
        if (!state.isCreated()) {
            cmd.replyTo().tell(new ServerEntityProtocol.ErrorResponse("NOT_FOUND", "Server does not exist"));
            return Effect().none();
        }

        if (state.isMember(cmd.userId())) {
            cmd.replyTo().tell(new ServerEntityProtocol.ErrorResponse("ALREADY_MEMBER", "User is already a member"));
            return Effect().none();
        }

        long now = Instant.now().toEpochMilli();
        var event = new ServerEntityProtocol.MemberAddedEvent(serverId, cmd.userId(), now);

        return Effect().persist(event).thenRun(() -> {
            log.info("User [{}] added to server [{}]", cmd.userId(), serverId);
            cmd.replyTo().tell(new ServerEntityProtocol.MemberAdded(serverId, cmd.userId()));

            cql.executeAsync(
                    "INSERT INTO " + CassandraSessionHolder.KEYSPACE + ".server_members (server_id, user_id, joined_at) VALUES (?, ?, ?)",
                    serverId, cmd.userId(), Instant.ofEpochMilli(now)
            ).whenComplete((r, err) -> {
                if (err != null) log.error("Failed to insert server_member [{}] into Cassandra", cmd.userId(), err);
            });

            EntityRef<UserEntityProtocol.Command> userEntity = sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, cmd.userId().toString());
            userEntity.tell(new UserEntityProtocol.NotifyServerCreated(serverId, state.getServerName()));
        });
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onRemoveMember(ServerState state, ServerEntityProtocol.RemoveMember cmd) {
        if (!state.isCreated()) {
            cmd.replyTo().tell(new ServerEntityProtocol.ErrorResponse("NOT_FOUND", "Server does not exist"));
            return Effect().none();
        }

        if (!state.isMember(cmd.userId())) {
            cmd.replyTo().tell(new ServerEntityProtocol.ErrorResponse("NOT_A_MEMBER", "User is not a member of this server"));
            return Effect().none();
        }

        long now = Instant.now().toEpochMilli();
        var event = new ServerEntityProtocol.MemberRemovedEvent(serverId, cmd.userId(), now);

        return Effect().persist(event).thenRun(() -> {
            log.info("User [{}] removed from server [{}]", cmd.userId(), serverId);
            cmd.replyTo().tell(new ServerEntityProtocol.MemberRemoved(serverId, cmd.userId()));
        });
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onCreateChannel(ServerState state, ServerEntityProtocol.CreateChannel cmd) {
        if (!state.isCreated()) {
            cmd.replyTo().tell(new ServerEntityProtocol.ErrorResponse("NOT_FOUND", "Server does not exist"));
            return Effect().none();
        }

        UUID channelId = UUID.randomUUID();
        long now = Instant.now().toEpochMilli();

        var event = new ServerEntityProtocol.ChannelCreatedEvent(serverId, channelId, cmd.channelName(), state.getOwnerId(), now);
        ActorRef<ChannelEntityProtocol.Response> adapter = ctx.messageAdapter(ChannelEntityProtocol.Response.class, ChannelResponseAdapter::new);

        return Effect().persist(event).thenRun(() -> {
            log.info("Channel [{}] created in server [{}]", channelId, serverId);
            EntityRef<ChannelEntityProtocol.Command> channelEntity = sharding.entityRefFor(ChannelEntityProtocol.ENTITY_KEY, channelId.toString());

            channelEntity.tell(new ChannelEntityProtocol.CreateChannel(serverId, null, cmd.channelName(), state.getOwnerId(), adapter));
            cmd.replyTo().tell(new ServerEntityProtocol.ChannelCreated(channelId));

            for (UUID memberId : state.getMembers()) {
                sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, memberId.toString())
                        .tell(new UserEntityProtocol.NotifyChannelCreated(serverId, channelId, cmd.channelName()));
            }
        });
    }

    private Effect<ServerEntityProtocol.Event, ServerState> onChannelResponse(ChannelResponseAdapter wrapped) {
        log.info("Received response from channel actor: {}", wrapped.response);
        return Effect().none();
    }
}
