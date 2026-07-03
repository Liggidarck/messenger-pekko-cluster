package com.messenger.actor;

import com.messenger.messages.auth.AuthTokenGeneratorMessages;
import com.messenger.messages.auth.UserCrudProtocol;
import com.messenger.utils.JwtUtil;
import com.messenger.utils.PasswordUtils;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.actor.typed.receptionist.Receptionist;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class AuthTokenGeneratorActor extends AbstractBehavior<AuthTokenGeneratorMessages.TokenCommand> {

    private final ActorRef<UserCrudProtocol.UserCrudCommand> unitsUserActor;

    private record WrappedUnitsResponse(UserCrudProtocol.UserResponse response, ActorRef<AuthTokenGeneratorMessages.TokenResponse> replyTo, String originalPassword) implements AuthTokenGeneratorMessages.TokenCommand {}

    public AuthTokenGeneratorActor(ActorContext<AuthTokenGeneratorMessages.TokenCommand> context, ActorRef<UserCrudProtocol.UserCrudCommand> unitsUserActor) {
        super(context);
        this.unitsUserActor = unitsUserActor;
        context.getSystem().receptionist().tell(Receptionist.register(AuthTokenGeneratorMessages.AUTH_TOKEN_GENERATOR_KEY, context.getSelf()));
    }

    public static Behavior<AuthTokenGeneratorMessages.TokenCommand> create(ActorRef<UserCrudProtocol.UserCrudCommand> unitsUserActor) {
        return Behaviors.setup(ctx -> new AuthTokenGeneratorActor(ctx, unitsUserActor));
    }

    @Override
    public Receive<AuthTokenGeneratorMessages.TokenCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(AuthTokenGeneratorMessages.GenerateToken.class, this::onGenerateToken)
                .onMessage(WrappedUnitsResponse.class, this::onUnitsResponse)
                .build();
    }

    private Behavior<AuthTokenGeneratorMessages.TokenCommand> onGenerateToken(AuthTokenGeneratorMessages.GenerateToken msg) {
        CompletionStage<UserCrudProtocol.UserResponse> askStage = AskPattern.ask(
                unitsUserActor,
                replyTo -> new UserCrudProtocol.LoginUser(msg.email(), replyTo),
                Duration.ofSeconds(3),
                getContext().getSystem().scheduler()
        );

        getContext().pipeToSelf(askStage, (response, error) -> {
            if (error != null) {
                return new WrappedUnitsResponse(new UserCrudProtocol.OperationFailed(error.getMessage()), msg.replyTo(), msg.password());
            }
            return new WrappedUnitsResponse(response, msg.replyTo(), msg.password());
        });

        return this;
    }

    private Behavior<AuthTokenGeneratorMessages.TokenCommand> onUnitsResponse(WrappedUnitsResponse wrapper) {
        ActorRef<AuthTokenGeneratorMessages.TokenResponse> clientReplyTo = wrapper.replyTo();

        switch (wrapper.response()) {
            case UserCrudProtocol.LoginSuccess user -> {
                if (PasswordUtils.checkPassword(wrapper.originalPassword(), user.hashPassword())) {
                    String token = JwtUtil.createToken(user.userView().email());
                    clientReplyTo.tell(new AuthTokenGeneratorMessages.TokenGenerateSuccess(token, System.currentTimeMillis() + 100_000));
                } else {
                    clientReplyTo.tell(new AuthTokenGeneratorMessages.TokenGenerateFailed("Invalid credentials"));
                }
            }
            case UserCrudProtocol.UserNotFound e -> clientReplyTo.tell(new AuthTokenGeneratorMessages.TokenGenerateFailed("Invalid credentials"));
            case UserCrudProtocol.OperationFailed e -> clientReplyTo.tell(new AuthTokenGeneratorMessages.TokenGenerateFailed("Service error: " + e.reason()));
            default -> clientReplyTo.tell(new AuthTokenGeneratorMessages.TokenGenerateFailed("Unknown error"));
        }
        return this;
    }
}
