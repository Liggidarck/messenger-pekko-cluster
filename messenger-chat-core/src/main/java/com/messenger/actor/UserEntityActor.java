package com.messenger.actor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.messenger.cassandra.CassandraSessionHolder;
import com.messenger.messages.core.UserEntityProtocol;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class UserEntityActor extends AbstractBehavior<UserEntityProtocol.Command> {

    private static final Logger logger = LoggerFactory.getLogger(UserEntityActor.class);

    private final UUID userId;
    private final CqlSession cql;
    private final Map<String, ActorRef<UserEntityProtocol.OutgoingMessage>> connectedSessions = new HashMap<>();

    public UserEntityActor(ActorContext<UserEntityProtocol.Command> context, EntityContext<UserEntityProtocol.Command> entityContext) {
        this(context, entityContext, CassandraSessionHolder.getSession());
    }

    public UserEntityActor(ActorContext<UserEntityProtocol.Command> context, EntityContext<UserEntityProtocol.Command> entityContext, CqlSession cql) {
        super(context);
        this.userId = UUID.fromString(entityContext.getEntityId());
        this.cql = cql;
        logger.info("UserEntity [{}] started on node", userId);
    }

    public static Behavior<UserEntityProtocol.Command> create(EntityContext<UserEntityProtocol.Command> entityContext) {
        return Behaviors.setup(context -> new UserEntityActor(context, entityContext));
    }

    @Override
    public Receive<UserEntityProtocol.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(UserEntityProtocol.DeviceConnected.class, this::onDeviceConnected)
                .onMessage(UserEntityProtocol.DeviceDisconnected.class, this::onDeviceDisconnected)
                .onMessage(UserEntityProtocol.DeliverMessage.class, this::onDeliverMessage)
                .onMessage(UserEntityProtocol.NotifyNewChannelMessage.class, this::onNotifyNewChannelMessage)
                .onMessage(UserEntityProtocol.NotifyEditMessage.class, this::onNotifyEditMessage)
                .onMessage(UserEntityProtocol.NotifyDeleteMessage.class, this::onNotifyDeleteMessage)
                .onMessage(UserEntityProtocol.NotifyServerCreated.class, this::onNotifyServerCreated)
                .onMessage(UserEntityProtocol.NotifyChannelCreated.class, this::onNotifyChannelCreated)
                .onMessage(UserEntityProtocol.GetServers.class, this::onGetServers)
                .onMessage(UserEntityProtocol.GetReadState.class, this::onGetReadState)
                .onMessage(UserEntityProtocol.UpdateReadState.class, this::onUpdateReadState)
                .onMessage(UserEntityProtocol.NotifyTyping.class, this::onNotifyTyping)
                .build();
    }

    private Behavior<UserEntityProtocol.Command> onNotifyTyping(UserEntityProtocol.NotifyTyping cmd) {
        String json = String.format(
                "{\"type\":\"user_typing\",\"channelId\":\"%s\",\"userId\":\"%s\"}",
                cmd.channelId(), cmd.userId()
        );
        connectedSessions.values().forEach(out -> out.tell(new UserEntityProtocol.OutgoingMessage(json)));
        return this;
    }

    private Behavior<UserEntityProtocol.Command> onDeviceConnected(UserEntityProtocol.DeviceConnected cmd) {
        logger.info("User [{}] device connected sessionId={}", userId, cmd.sessionId());
        connectedSessions.put(cmd.sessionId(), cmd.replyTo());
        cmd.replyTo().tell(new UserEntityProtocol.OutgoingMessage("connected"));
        return this;
    }

    private Behavior<UserEntityProtocol.Command> onDeviceDisconnected(UserEntityProtocol.DeviceDisconnected cmd) {
        logger.info("User [{}] device disconnected sessionId={}", userId, cmd.sessionId());
        connectedSessions.remove(cmd.sessionId());
        return this;
    }

    private Behavior<UserEntityProtocol.Command> onDeliverMessage(UserEntityProtocol.DeliverMessage cmd) {
        logger.info("User [{}] received message from [{}]: {}", userId, cmd.fromUserId(), cmd.text());
        String formatted = "From " + cmd.fromUserId() + ": " + cmd.text();
        if (connectedSessions.isEmpty()) {
            logger.info("User [{}] has no connected sessions, message queued (not implemented)", userId);
        } else {
            for (ActorRef<UserEntityProtocol.OutgoingMessage> out : connectedSessions.values()) {
                out.tell(new UserEntityProtocol.OutgoingMessage(formatted));
            }
        }
        return this;
    }

    private Behavior<UserEntityProtocol.Command> onNotifyNewChannelMessage(UserEntityProtocol.NotifyNewChannelMessage cmd) {
        logger.info("User [{}] notification from channel [{}] from [{}]", userId, cmd.channelId(), cmd.fromUserId());
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"new_message\",\"serverId\":\"")
                .append(cmd.serverId()).append("\",\"channelId\":\"")
                .append(cmd.channelId()).append("\",\"message\":{")
                .append("\"messageId\":\"").append(cmd.messageId()).append("\",")
                .append("\"senderId\":\"").append(cmd.fromUserId()).append("\",")
                .append("\"text\":\"").append(escapeJson(cmd.text())).append("\",")
                .append("\"createdAt\":").append(cmd.createdAt()).append(",")
                .append("\"mediaUrls\":[");
        if (cmd.mediaUrls() != null) {
            boolean first = true;
            for (String url : cmd.mediaUrls()) {
                if (!first) sb.append(",");
                sb.append("\"").append(escapeJson(url)).append("\"");
                first = false;
            }
        }
        sb.append("],\"replyToMessageId\":");
        if (cmd.replyToMessageId() != null) {
            sb.append("\"").append(cmd.replyToMessageId()).append("\"");
        } else {
            sb.append("null");
        }
        sb.append("}}");
        String json = sb.toString();
        for (ActorRef<UserEntityProtocol.OutgoingMessage> out : connectedSessions.values()) {
            out.tell(new UserEntityProtocol.OutgoingMessage(json));
        }
        return this;
    }

    private Behavior<UserEntityProtocol.Command> onNotifyEditMessage(UserEntityProtocol.NotifyEditMessage cmd) {
        logger.info("User [{}] edit notification for message [{}] in channel [{}]", userId, cmd.messageId(), cmd.channelId());
        String json = String.format(
                "{\"type\":\"message_edited\",\"serverId\":\"%s\",\"channelId\":\"%s\",\"messageId\":\"%s\",\"text\":\"%s\",\"editedAt\":%d}",
                cmd.serverId(), cmd.channelId(), cmd.messageId(), escapeJson(cmd.text()), cmd.editedAt()
        );
        connectedSessions.values().forEach(out -> out.tell(new UserEntityProtocol.OutgoingMessage(json)));
        return this;
    }

    private Behavior<UserEntityProtocol.Command> onNotifyDeleteMessage(UserEntityProtocol.NotifyDeleteMessage cmd) {
        logger.info("User [{}] delete notification for message [{}] in channel [{}]", userId, cmd.messageId(), cmd.channelId());
        String json = String.format(
                "{\"type\":\"message_deleted\",\"serverId\":\"%s\",\"channelId\":\"%s\",\"messageId\":\"%s\"}",
                cmd.serverId(), cmd.channelId(), cmd.messageId()
        );
        connectedSessions.values().forEach(out -> out.tell(new UserEntityProtocol.OutgoingMessage(json)));
        return this;
    }

    private Behavior<UserEntityProtocol.Command> onNotifyServerCreated(UserEntityProtocol.NotifyServerCreated cmd) {
        logger.info("User [{}] notification of a new server [{}]", userId, cmd.serverId());

        cql.executeAsync(
                "INSERT INTO " + CassandraSessionHolder.KEYSPACE + ".servers_by_user (user_id, server_id, joined_at) VALUES (?, ?, ?)",
                userId, cmd.serverId(), Instant.now()
        ).whenComplete((result, err) -> {
            if (err != null) {
                logger.error("Failed to save server [{}] to Cassandra for user [{}]", cmd.serverId(), userId, err);
            }
        });

        String json = String.format(
                "{\"type\":\"server_created\",\"serverId\":\"%s\",\"serverName\":\"%s\"}",
                cmd.serverId(), escapeJson(cmd.serverName())
        );
        for (ActorRef<UserEntityProtocol.OutgoingMessage> out : connectedSessions.values()) {
            out.tell(new UserEntityProtocol.OutgoingMessage(json));
        }
        return this;
    }

    private Behavior<UserEntityProtocol.Command> onNotifyChannelCreated(UserEntityProtocol.NotifyChannelCreated cmd) {
        logger.info("User [{}] notification of a new channel [{}] in server [{}]", userId, cmd.channelId(), cmd.serverId());
        String json = String.format(
                "{\"type\":\"channel_created\",\"serverId\":\"%s\",\"channelId\":\"%s\",\"channelName\":\"%s\"}",
                cmd.serverId(), cmd.channelId(), escapeJson(cmd.channelName())
        );
        for (ActorRef<UserEntityProtocol.OutgoingMessage> out : connectedSessions.values()) {
            out.tell(new UserEntityProtocol.OutgoingMessage(json));
        }
        return this;
    }


    private Behavior<UserEntityProtocol.Command> onGetServers(UserEntityProtocol.GetServers cmd) {
        cql.executeAsync(
                "SELECT server_id FROM " + CassandraSessionHolder.KEYSPACE + ".servers_by_user WHERE user_id = ? LIMIT 200",
                userId
        ).thenCompose(rows -> {
            List<UUID> serverIds = new ArrayList<>();
            for (Row row : rows.currentPage()) {
                serverIds.add(row.getUuid("server_id"));
            }
            if (serverIds.isEmpty()) {
                return CompletableFuture.completedFuture(new UserEntityProtocol.GetServersResponse(List.of()));
            }
            List<CompletableFuture<UserEntityProtocol.ServerDto>> futures = new ArrayList<>();
            for (UUID sid : serverIds) {
                CompletableFuture<UserEntityProtocol.ServerDto> f = cql.executeAsync(
                        "SELECT name, owner_id, created_at FROM " + CassandraSessionHolder.KEYSPACE + ".servers WHERE server_id = ?",
                        sid
                ).thenApply(r -> {
                    Row row = r.one();
                    if (row != null) {
                        return new UserEntityProtocol.ServerDto(
                                sid,
                                row.getString("name"),
                                row.getUuid("owner_id"),
                                row.getInstant("created_at") != null ? row.getInstant("created_at").toEpochMilli() : 0
                        );
                    }
                    return new UserEntityProtocol.ServerDto(sid, "Unknown", null, 0);
                }).toCompletableFuture();
                futures.add(f);
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        List<UserEntityProtocol.ServerDto> result = new ArrayList<>();
                        for (CompletableFuture<UserEntityProtocol.ServerDto> f : futures) {
                            result.add(f.join());
                        }
                        return new UserEntityProtocol.GetServersResponse(result);
                    });
        }).whenComplete((response, err) -> {
            if (err != null) {
                logger.error("Failed to query servers for user [{}]", userId, err);
                cmd.replyTo().tell(new UserEntityProtocol.GetServersResponse(List.of()));
            } else {
                cmd.replyTo().tell(response);
            }
        });
        return this;
    }

    private void sendReadState(UUID channelId, UUID lastRead, ActorRef<UserEntityProtocol.ReadStateResponse> replyTo) {
        String query;
        if (lastRead == null) {
            query = "SELECT COUNT(*) FROM " + CassandraSessionHolder.KEYSPACE + ".messages WHERE channel_id = ? LIMIT 100";
        } else {
            query = "SELECT COUNT(*) FROM " + CassandraSessionHolder.KEYSPACE + ".messages WHERE channel_id = ? AND message_id > ? LIMIT 100";
        }
        cql.executeAsync(
                query,
                lastRead != null ? new Object[]{channelId, lastRead} : new Object[]{channelId}
        ).whenComplete((countRows, err) -> {
            int count = 0;
            if (err != null) {
                logger.error("Failed to count unread for channel [{}]", channelId, err);
            } else {
                Row r = countRows.one();
                if (r != null) count = (int) r.getLong(0);
            }
            int unread = count >= 100 ? 99 : count;
            replyTo.tell(new UserEntityProtocol.ReadStateResponse(channelId, lastRead, unread));
        });
    }

    private Behavior<UserEntityProtocol.Command> onGetReadState(UserEntityProtocol.GetReadState cmd) {
        cql.executeAsync(
                "SELECT last_read_message_id FROM " + CassandraSessionHolder.KEYSPACE + ".channel_read_states WHERE user_id = ? AND channel_id = ?",
                userId, cmd.channelId()
        ).whenComplete((rows, err) -> {
            if (err != null) {
                logger.error("Failed to get read state for user [{}] channel [{}]", userId, cmd.channelId(), err);
                sendReadState(cmd.channelId(), null, cmd.replyTo());
                return;
            }
            Row row = rows.one();
            UUID lastRead = row != null ? row.getUuid("last_read_message_id") : null;
            sendReadState(cmd.channelId(), lastRead, cmd.replyTo());
        });
        return this;
    }

    private Behavior<UserEntityProtocol.Command> onUpdateReadState(UserEntityProtocol.UpdateReadState cmd) {
        cql.executeAsync(
                "INSERT INTO " + CassandraSessionHolder.KEYSPACE + ".channel_read_states (user_id, channel_id, last_read_message_id) VALUES (?, ?, ?)",
                userId, cmd.channelId(), cmd.lastReadMessageId()
        ).whenComplete((r, err) -> {
            if (err != null) {
                logger.error("Failed to update read state for user [{}] channel [{}]", userId, cmd.channelId(), err);
            }
            sendReadState(cmd.channelId(), cmd.lastReadMessageId(), cmd.replyTo());
        });
        return this;
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
