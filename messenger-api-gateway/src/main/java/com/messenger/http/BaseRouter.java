package com.messenger.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messenger.messages.HttpFinalResponse;
import com.messenger.messages.auth.AuthValidatorMessages;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpEntities;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.Route;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class BaseRouter extends AllDirectives {

    protected static final ObjectMapper mapper = new ObjectMapper();

    protected final ActorSystem<?> system;
    protected final ActorRef<AuthValidatorMessages.ValidateToken> validatorActor;

    protected BaseRouter(ActorSystem<?> actorSystem, ActorRef<AuthValidatorMessages.ValidateToken> validatorActor) {
        this.system = actorSystem;
        this.validatorActor = validatorActor;
    }

    public abstract Route createRoutes();

    protected Route authenticated(Function<User, Route> innerRoute) {
        return optionalHeaderValueByName("Authorization", tokenOpt -> {

            if (tokenOpt.isEmpty()) {
                return completeHttp(new HttpFinalResponse(401, mapper.createObjectNode().put("error", "Missing Token")));
            }

            String token = tokenOpt.get().replace("Bearer ", "");
            CompletionStage<AuthValidatorMessages.AuthValidatorResult> authFuture = AskPattern.ask(
                    validatorActor,
                    replyTo -> new AuthValidatorMessages.ValidateToken(token, replyTo),
                    Duration.ofSeconds(3),
                    system.scheduler()
            );

            return onSuccess(authFuture, result -> {
                if (result instanceof AuthValidatorMessages.TokenValid(String userEmail, UUID userId)) {
                    return innerRoute.apply(new User(userEmail, userId));
                } else {
                    return completeHttp(new HttpFinalResponse(401, mapper.createObjectNode().put("error", "Invalid Token")));
                }
            });
        });
    }

    public static class User {
        public String email;
        public UUID userId;

        public User(String email, UUID userId) {
            this.email = email;
            this.userId = userId;
        }
    }

    protected Route completeHttp(HttpFinalResponse result) {
        return complete(
                StatusCodes.get(result.statusCode()),
                HttpEntities.create(ContentTypes.APPLICATION_JSON, result.body().toString())
        );
    }
}
