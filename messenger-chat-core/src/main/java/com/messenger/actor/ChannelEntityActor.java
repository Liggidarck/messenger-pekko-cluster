package com.messenger.actor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.messenger.cassandra.CassandraSessionHolder;
import com.messenger.messages.core.ChannelEntityProtocol;
import com.messenger.messages.core.ServerEntityProtocol;
import com.messenger.messages.core.UserEntityProtocol;
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

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ChannelEntityActor extends EventSourcedBehavior<ChannelEntityProtocol.Command, ChannelEntityProtocol.Event, ChannelState> {
    private static final Logger log = LoggerFactory.getLogger(ChannelEntityActor.class);

    private final ActorContext<ChannelEntityProtocol.Command> ctx;
    private final ClusterSharding sharding;
    private final CqlSession cql;
    private final UUID channelId;

    public ChannelEntityActor(PersistenceId persistenceId, ActorContext<ChannelEntityProtocol.Command> ctx, ClusterSharding sharding, CqlSession cql) {
        super(persistenceId);
        this.ctx = ctx;
        this.sharding = sharding;
        this.cql = cql;
        this.channelId = UUID.fromString(persistenceId.entityId());
    }

    public static Behavior<ChannelEntityProtocol.Command> create(EntityContext<ChannelEntityProtocol.Command> entityContext) {
        UUID channelId = UUID.fromString(entityContext.getEntityId());
        PersistenceId persistenceId = PersistenceId.of("ChannelEntity", channelId.toString());
        return Behaviors.setup(ctx -> {
            ClusterSharding sharding = ClusterSharding.get(ctx.getSystem());
            CqlSession cql = CassandraSessionHolder.getSession();
            return new ChannelEntityActor(persistenceId, ctx, sharding, cql);
        });
    }

    @Override
    public ChannelState emptyState() {
        return ChannelState.empty(channelId);
    }

    @Override
    public CommandHandler<ChannelEntityProtocol.Command, ChannelEntityProtocol.Event, ChannelState> commandHandler() {
        return newCommandHandlerBuilder()
                .forAnyState()
                .onCommand(ChannelEntityProtocol.CreateChannel.class, this::onCreateChannel)
                .onCommand(ChannelEntityProtocol.SendMessage.class, this::onSendMessage)
                .onCommand(ChannelEntityProtocol.EditMessage.class, this::onEditMessage)
                .onCommand(ChannelEntityProtocol.DeleteMessage.class, this::onDeleteMessage)
                .onCommand(ChannelEntityProtocol.GetHistory.class, this::onGetHistory)
                .onCommand(ChannelEntityProtocol.GetHistoryResult.class, this::onGetHistoryResult)
                .onCommand(ChannelEntityProtocol.Typing.class, this::onTyping)
                .build();
    }

    private Effect<ChannelEntityProtocol.Event, ChannelState> onTyping(ChannelState state, ChannelEntityProtocol.Typing cmd) {
        if (!state.isCreated()) {
            return Effect().none();
        }

        if (state.isDirect()) {
            for (UUID memberId : state.getDirectMembers()) {
                if (!memberId.equals(cmd.userId())) {
                    sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, memberId.toString())
                            .tell(new UserEntityProtocol.NotifyTyping(channelId, cmd.userId()));
                }
            }
        } else {
            if (state.getServerId() != null) {
                sharding.entityRefFor(ServerEntityProtocol.ENTITY_KEY, state.getServerId().toString())
                        .tell(new ServerEntityProtocol.BroadcastTyping(channelId, cmd.userId()));
            }
        }

        return Effect().none();
    }

    @Override
    public EventHandler<ChannelState, ChannelEntityProtocol.Event> eventHandler() {
        return (state, evt) -> state.applyEvent(evt);
    }

    private Effect<ChannelEntityProtocol.Event, ChannelState> onCreateChannel(ChannelState state, ChannelEntityProtocol.CreateChannel cmd) {
        if (state.isCreated()) {
            cmd.replyTo().tell(new ChannelEntityProtocol.ErrorResponse("ALREADY_EXISTS", "Channel already exists"));
            return Effect().none();
        }

        long now = Instant.now().toEpochMilli();

        var event = new ChannelEntityProtocol.ChannelCreatedEvent(
                cmd.serverId(), channelId, cmd.directMembers(), cmd.title(), cmd.createdBy(), now);

        return Effect().persist(event).thenRun(() -> {
            log.info("Channel [{}] created", channelId);
            cmd.replyTo().tell(new ChannelEntityProtocol.ChannelCreated(channelId));
        });
    }


    private Effect<ChannelEntityProtocol.Event, ChannelState> onSendMessage(ChannelState state, ChannelEntityProtocol.SendMessage cmd) {
        if (!state.isCreated()) {
            cmd.replyTo().tell(new ChannelEntityProtocol.ErrorResponse("CHANNEL_NOT_FOUND", "Channel does not exist"));
            return Effect().none();
        }

        UUID messageId = Uuids.timeBased();
        long now = Instant.now().toEpochMilli();
        var event = new ChannelEntityProtocol.MessageSentEvent(
                cmd.channelId(), messageId, cmd.senderId(), cmd.text(), cmd.mediaUrls(), cmd.replyToMessageId(), now);

        return Effect().persist(event).thenRun(() -> {
            log.info("Message [{}] in channel [{}] persisted to Event Journal", messageId, cmd.channelId());
            cmd.replyTo().tell(new ChannelEntityProtocol.MessageSent(messageId, now));

            cql.executeAsync(
                    "INSERT INTO " + CassandraSessionHolder.KEYSPACE + ".messages " +
                            "(channel_id, message_id, sender_id, text, media_urls, reply_to_message_id, is_edited, is_deleted) " +
                            "VALUES (?, ?, ?, ?, ?, ?, false, false)",
                    cmd.channelId(), messageId, cmd.senderId(), cmd.text(), cmd.mediaUrls(), cmd.replyToMessageId()
            ).whenComplete((result, err) -> {
                if (err != null) {
                    log.error("Failed to write message [{}] to Cassandra read model", messageId, err);
                }
            });

            if (state.isDirect()) {
                for (UUID memberId : state.getDirectMembers()) {
                    EntityRef<UserEntityProtocol.Command> userEntity =
                            sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, memberId.toString());
                    userEntity.tell(new UserEntityProtocol.NotifyNewChannelMessage(
                            null, cmd.channelId(), messageId, cmd.senderId(), cmd.text(), now, cmd.mediaUrls(), cmd.replyToMessageId()));
                }
            } else {
                EntityRef<ServerEntityProtocol.Command> serverEntity =
                        sharding.entityRefFor(ServerEntityProtocol.ENTITY_KEY, state.getServerId().toString());
                serverEntity.tell(new ServerEntityProtocol.BroadcastChannelMessage(
                        cmd.channelId(), messageId, cmd.senderId(), cmd.text(), now, cmd.mediaUrls(), cmd.replyToMessageId()));
            }
        });
    }

    private Effect<ChannelEntityProtocol.Event, ChannelState> onEditMessage(ChannelState state, ChannelEntityProtocol.EditMessage cmd) {
        if (!state.isCreated()) {
            cmd.replyTo().tell(new ChannelEntityProtocol.ErrorResponse("CHANNEL_NOT_FOUND", "Channel does not exist"));
            return Effect().none();
        }

        long now = Instant.now().toEpochMilli();
        var event = new ChannelEntityProtocol.EditMessageEvent(cmd.channelId(), cmd.messageId(), cmd.senderId(), cmd.text(), now);

        return Effect().persist(event).thenRun(() -> {
            log.info("Message [{}] edited in channel [{}]", cmd.messageId(), cmd.channelId());
            cmd.replyTo().tell(new ChannelEntityProtocol.MessageEdited(cmd.messageId(), now));

            cql.executeAsync(
                    "UPDATE " + CassandraSessionHolder.KEYSPACE + ".messages " +
                            "SET text = ?, is_edited = true, edited_at = ? " +
                            "WHERE channel_id = ? AND message_id = ?",
                    cmd.text(), Instant.ofEpochMilli(now), cmd.channelId(), cmd.messageId()
            ).whenComplete((result, err) -> {
                if (err != null) {
                    log.error("Failed to update message [{}] in Cassandra", cmd.messageId(), err);
                }
            });

            if (!state.isDirect() && state.getServerId() != null) {
                sharding.entityRefFor(ServerEntityProtocol.ENTITY_KEY, state.getServerId().toString())
                        .tell(new ServerEntityProtocol.BroadcastEditMessage(cmd.channelId(), cmd.messageId(), cmd.text(), now));
            } else {
                for (UUID memberId : state.getDirectMembers()) {
                    sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, memberId.toString())
                            .tell(new UserEntityProtocol.NotifyEditMessage(null, cmd.channelId(), cmd.messageId(), cmd.text(), now));
                }
            }
        });
    }

    private Effect<ChannelEntityProtocol.Event, ChannelState> onDeleteMessage(ChannelState state, ChannelEntityProtocol.DeleteMessage cmd) {
        if (!state.isCreated()) {
            cmd.replyTo().tell(new ChannelEntityProtocol.ErrorResponse("CHANNEL_NOT_FOUND", "Channel does not exist"));
            return Effect().none();
        }

        var event = new ChannelEntityProtocol.DeleteMessageEvent(cmd.channelId(), cmd.messageId(), cmd.senderId());

        return Effect().persist(event).thenRun(() -> {
            log.info("Message [{}] deleted in channel [{}]", cmd.messageId(), cmd.channelId());
            cmd.replyTo().tell(new ChannelEntityProtocol.MessageDeleted(cmd.messageId()));

            cql.executeAsync(
                    "UPDATE " + CassandraSessionHolder.KEYSPACE + ".messages " +
                            "SET text = '', is_deleted = true " +
                            "WHERE channel_id = ? AND message_id = ?",
                    cmd.channelId(), cmd.messageId()
            ).whenComplete((result, err) -> {
                if (err != null) {
                    log.error("Failed to delete message [{}] in Cassandra", cmd.messageId(), err);
                }
            });

            if (!state.isDirect() && state.getServerId() != null) {
                sharding.entityRefFor(ServerEntityProtocol.ENTITY_KEY, state.getServerId().toString())
                        .tell(new ServerEntityProtocol.BroadcastDeleteMessage(cmd.channelId(), cmd.messageId()));
            } else {
                for (UUID memberId : state.getDirectMembers()) {
                    sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, memberId.toString())
                            .tell(new UserEntityProtocol.NotifyDeleteMessage(null, cmd.channelId(), cmd.messageId()));
                }
            }
        });
    }

    private Effect<ChannelEntityProtocol.Event, ChannelState> onGetHistory(ChannelState state, ChannelEntityProtocol.GetHistory cmd) {
        if (!state.isCreated()) {
            cmd.replyTo().tell(new ChannelEntityProtocol.ErrorResponse("CHANNEL_NOT_FOUND", "Channel does not exist"));
            return Effect().none();
        }

        String query;
        Object[] bindParams;

        if (cmd.beforeMessageId() != null) {
            query = "SELECT message_id, sender_id, text, media_urls, reply_to_message_id, is_edited, edited_at, is_deleted" +
                    " FROM " + CassandraSessionHolder.KEYSPACE + ".messages" +
                    " WHERE channel_id = ? AND message_id < ? LIMIT ?";
            bindParams = new Object[]{channelId, cmd.beforeMessageId(), cmd.limit()};
        } else {
            query = "SELECT message_id, sender_id, text, media_urls, reply_to_message_id, is_edited, edited_at, is_deleted" +
                    " FROM " + CassandraSessionHolder.KEYSPACE + ".messages" +
                    " WHERE channel_id = ? LIMIT ?";
            bindParams = new Object[]{channelId, cmd.limit()};
        }

        CompletableFuture<AsyncResultSet> future = cql.executeAsync(query, bindParams).toCompletableFuture();

        ctx.pipeToSelf(future, (result, err) -> {
            if (err != null) {
                log.error("Failed to fetch history for channel [{}]", channelId, err);
                return new ChannelEntityProtocol.GetHistoryResult(List.of(), false, cmd.replyTo());
            }
            List<ChannelEntityProtocol.MessageDto> messages = new ArrayList<>();
            for (var row : result.currentPage()) {
                UUID messageId = row.getUuid("message_id");
                messages.add(new ChannelEntityProtocol.MessageDto(
                        messageId,
                        row.getUuid("sender_id"),
                        row.getString("text"),
                        com.datastax.oss.driver.api.core.uuid.Uuids.unixTimestamp(messageId),
                        row.getList("media_urls", String.class),
                        row.getUuid("reply_to_message_id"),
                        row.getBoolean("is_edited"),
                        row.getInstant("edited_at") != null ? row.getInstant("edited_at").toEpochMilli() : null,
                        row.getBoolean("is_deleted")
                ));
            }
            return new ChannelEntityProtocol.GetHistoryResult(messages, result.hasMorePages() || messages.size() == cmd.limit(), cmd.replyTo());
        });

        return Effect().none();
    }

    private Effect<ChannelEntityProtocol.Event, ChannelState> onGetHistoryResult(ChannelState state, ChannelEntityProtocol.GetHistoryResult cmd) {
        cmd.replyTo().tell(new ChannelEntityProtocol.HistoryResponse(cmd.messages(), cmd.hasMore()));
        return Effect().none();
    }
}
