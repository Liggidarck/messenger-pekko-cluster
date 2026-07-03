package com.messenger.actor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messenger.messages.core.ChannelEntityProtocol;
import com.messenger.messages.core.SessionProtocol;
import com.messenger.messages.core.UserEntityProtocol;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class UserSessionActor extends AbstractBehavior<SessionProtocol.SessionCommand> {
    private static final Logger logger = LoggerFactory.getLogger(UserSessionActor.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Set<ActorRef<UserEntityProtocol.OutgoingMessage>> activeWebSockets = ConcurrentHashMap.newKeySet();

    private final ActorRef<UserEntityProtocol.OutgoingMessage> webSocketOut;
    private final ClusterSharding sharding;
    private final UUID currentUserId;
    private final String sessionId;

    public UserSessionActor(ActorContext<SessionProtocol.SessionCommand> context,
                            ActorRef<UserEntityProtocol.OutgoingMessage> webSocketOut,
                            UUID currentUserId,
                            ClusterSharding sharding) {
        super(context);
        this.webSocketOut = webSocketOut;
        this.currentUserId = currentUserId;
        this.sharding = sharding;
        this.sessionId = context.getSelf().path().name();

        activeWebSockets.add(webSocketOut);

        EntityRef<UserEntityProtocol.Command> entity = sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, currentUserId.toString());
        entity.tell(new UserEntityProtocol.DeviceConnected(sessionId, webSocketOut));
        logger.info("User with ID [{}] connected via secure WebSocket. SessionId: {}", currentUserId, sessionId);
    }

    public static Behavior<SessionProtocol.SessionCommand> create(
            ActorRef<UserEntityProtocol.OutgoingMessage> webSocketOut,
            UUID currentUserId,
            ClusterSharding sharding) {
        return Behaviors.setup(context -> new UserSessionActor(context, webSocketOut, currentUserId, sharding));
    }

    @Override
    public Receive<SessionProtocol.SessionCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(SessionProtocol.IncomingClientMessage.class, this::onIncomingMessage)
                .onMessage(SessionProtocol.OutgoingClientMessage.class, this::onOutgoingMessage)
                .onMessage(SessionProtocol.ConnectionClosed.class, this::onConnectionClosed)
                .build();
    }

    private Behavior<SessionProtocol.SessionCommand> onIncomingMessage(SessionProtocol.IncomingClientMessage msg) {
        String payload = msg.payload().trim();
        logger.debug("Received message: {}", payload);

        if (payload.startsWith("{")) {
            handleJson(payload);
        } else {
            logger.debug("Ignored raw text command: {}", payload);
        }
        return this;
    }

    private void handleJson(String payload) {
        try {
            Map<String, Object> json = parseJson(payload);
            String type = (String) json.get("type");
            if (type == null) return;

            switch (type) {
                case "typing" -> handleTyping(json);
                case "ping" -> { /* keep-alive */ }
                default -> logger.debug("Unknown JSON type: {}", type);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse JSON: {}", e.getMessage());
        }
    }

    private void handleTyping(Map<String, Object> json) {
        String channelIdStr = (String) json.get("channelId");
        if (channelIdStr == null) return;

        sharding.entityRefFor(ChannelEntityProtocol.ENTITY_KEY, channelIdStr)
                .tell(new ChannelEntityProtocol.Typing(currentUserId, getContext().getSystem().ignoreRef()));
    }

    private Behavior<SessionProtocol.SessionCommand> onOutgoingMessage(SessionProtocol.OutgoingClientMessage msg) {
        webSocketOut.tell(new UserEntityProtocol.OutgoingMessage(msg.payload()));
        return this;
    }

    private Behavior<SessionProtocol.SessionCommand> onConnectionClosed(SessionProtocol.ConnectionClosed msg) {
        logger.info("WebSocket session [{}] closed", sessionId);
        activeWebSockets.remove(webSocketOut);
        if (currentUserId != null) {
            EntityRef<UserEntityProtocol.Command> entity = sharding.entityRefFor(UserEntityProtocol.ENTITY_KEY, currentUserId.toString());
            entity.tell(new UserEntityProtocol.DeviceDisconnected(sessionId));
        }
        return Behaviors.stopped();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseJson(String json) {
        try {
            return mapper.readValue(json, LinkedHashMap.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
