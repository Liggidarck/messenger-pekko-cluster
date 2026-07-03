package com.messenger.http;

import com.messenger.messages.auth.AuthValidatorMessages;
import com.messenger.actor.UserSessionActor;
import com.messenger.messages.core.SessionProtocol;
import com.messenger.messages.core.UserEntityProtocol;
import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Props;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.model.ws.Message;
import org.apache.pekko.http.javadsl.model.ws.TextMessage;
import org.apache.pekko.http.javadsl.server.Route;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.stream.typed.javadsl.ActorSink;
import org.apache.pekko.stream.typed.javadsl.ActorSource;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class WebSocketRouter extends BaseRouter {

    private final ClusterSharding sharding;

    public WebSocketRouter(ActorSystem<?> actorSystem, ActorRef<AuthValidatorMessages.ValidateToken> validatorActor, ClusterSharding sharding) {
        super(actorSystem, validatorActor);
        this.sharding = sharding;
    }

    @Override
    public Route createRoutes() {
        return path("chat", () ->
                parameter("token", token -> {
                    CompletionStage<AuthValidatorMessages.AuthValidatorResult> authResult = AskPattern.ask(
                            validatorActor,
                            replyTo -> new AuthValidatorMessages.ValidateToken(token, replyTo),
                            Duration.ofSeconds(3),
                            system.scheduler()
                    );

                    return onSuccess(authResult, result -> {
                        if (result instanceof AuthValidatorMessages.TokenValid valid) {
                            return handleWebSocketMessages(createWebSocketFlow(system, valid.userId(), sharding));
                        } else {
                            return complete(StatusCodes.UNAUTHORIZED, "Invalid Token");
                        }
                    });
                })
        );
    }

    private Flow<Message, Message, NotUsed> createWebSocketFlow(ActorSystem<?> system,
                                                                UUID userId,
                                                                ClusterSharding sharding) {

        Source<Message, ActorRef<UserEntityProtocol.OutgoingMessage>> wsSource = ActorSource.<UserEntityProtocol.OutgoingMessage>actorRef(
                msg -> false,
                msg -> Optional.empty(),
                100,
                OverflowStrategy.dropHead()
        ).map(msg -> TextMessage.create(msg.payload()));

        Pair<ActorRef<UserEntityProtocol.OutgoingMessage>, Source<Message, NotUsed>> pair = wsSource.preMaterialize(system);
        ActorRef<UserEntityProtocol.OutgoingMessage> webSocketOutActor = pair.first();
        Source<Message, NotUsed> outgoingSource = pair.second();

        String sessionId = UUID.randomUUID().toString().substring(0, 8);
        ActorRef<SessionProtocol.SessionCommand> sessionActor = system.systemActorOf(
                UserSessionActor.create(webSocketOutActor, userId, sharding),
                "session-" + sessionId,
                Props.empty()
        );

        Sink<Message, NotUsed> wsSink = Flow.<Message>create()
                .map(msg -> {
                    SessionProtocol.SessionCommand command;
                    if (msg.isText()) {
                        command = new SessionProtocol.IncomingClientMessage(msg.asTextMessage().getStrictText());
                    } else {
                        command = new SessionProtocol.IncomingClientMessage("");
                    }
                    return command;
                })
                .to(ActorSink.actorRef(
                        sessionActor,
                        new SessionProtocol.ConnectionClosed(),
                        e -> new SessionProtocol.ConnectionClosed()
                ));

        return Flow.fromSinkAndSource(wsSink, outgoingSource);
    }
}