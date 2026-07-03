package com.messenger.actor;

import com.messenger.messages.auth.AuthValidatorMessages;
import com.messenger.models.User;
import com.messenger.repository.UserRepository;
import com.messenger.utils.JwtUtil;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.DispatcherSelector;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;


public class AuthValidatorActor extends AbstractBehavior<AuthValidatorMessages.ValidateToken> {

    private static final Logger logger = LoggerFactory.getLogger(AuthValidatorActor.class);

    private final UserRepository userRepository;
    private final Executor jdbcExecutor;

    public AuthValidatorActor(ActorContext<AuthValidatorMessages.ValidateToken> context, UserRepository userRepository) {
        super(context);
        logger.info("Auth Worker started and registering in Receptionist");

        context.getSystem().receptionist().tell(
                Receptionist.register(AuthValidatorMessages.AUTH_VALIDATOR_KEY, context.getSelf())
        );
        this.userRepository = userRepository;
        this.jdbcExecutor = context.getSystem().dispatchers().lookup(DispatcherSelector.fromConfig("jdbc-dispatcher"));
    }

    public static Behavior<AuthValidatorMessages.ValidateToken> create(UserRepository userRepository) {
        return Behaviors.setup(context -> new AuthValidatorActor(context, userRepository));
    }

    @Override
    public Receive<AuthValidatorMessages.ValidateToken> createReceive() {
        return newReceiveBuilder()
                .onMessage(AuthValidatorMessages.ValidateToken.class, this::onValidateToken)
                .build();
    }

    private Behavior<AuthValidatorMessages.ValidateToken> onValidateToken(AuthValidatorMessages.ValidateToken message) {
        String token = message.token();

        if (!JwtUtil.isValid(token)) {
            message.replyTo().tell(new AuthValidatorMessages.TokenInvalid("invalid token"));
            return this;
        }

        String email = JwtUtil.getSubject(token);
        CompletableFuture<AuthValidatorMessages.AuthValidatorResult> future = CompletableFuture.supplyAsync(() -> {
            User user = userRepository.findByEmail(email);
            if (user != null) {
                return new AuthValidatorMessages.TokenValid(user.getEmail(), user.getId());
            }
            return new AuthValidatorMessages.TokenInvalid("user not found");
        }, jdbcExecutor);

        future.thenAccept(message.replyTo()::tell);
        return this;
    }
}
