package com.messenger.http;

import com.messenger.actor.CreateUserFlow;
import com.messenger.messages.HttpFinalResponse;
import com.messenger.messages.auth.AuthPasswordHashMessages;
import com.messenger.messages.auth.AuthTokenGeneratorMessages;
import com.messenger.messages.auth.AuthValidatorMessages;
import com.messenger.messages.auth.UserCrudProtocol;
import com.messenger.requests.CreateUserRequest;
import com.messenger.requests.LoginRequest;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Props;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.http.javadsl.marshallers.jackson.Jackson;
import org.apache.pekko.http.javadsl.server.Route;
import static org.apache.pekko.http.javadsl.server.PathMatchers.uuidSegment;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class AuthRouter extends BaseRouter {

    private final ActorRef<AuthTokenGeneratorMessages.TokenCommand> tokenGeneratorActor;
    private final ActorRef<UserCrudProtocol.UserCrudCommand> unitsUserActor;
    private final ActorRef<AuthPasswordHashMessages.StartHashPasswd> passwordHasherActor;

    protected AuthRouter(ActorSystem<?> system,
                         ActorRef<AuthValidatorMessages.ValidateToken> validateTokenActor,
                         ActorRef<AuthTokenGeneratorMessages.TokenCommand> tokenGeneratorActor,
                         ActorRef<AuthPasswordHashMessages.StartHashPasswd> passwordHasherActor,
                         ActorRef<UserCrudProtocol.UserCrudCommand> unitsUserActor) {
        super(system, validateTokenActor);
        this.tokenGeneratorActor = tokenGeneratorActor;
        this.unitsUserActor = unitsUserActor;
        this.passwordHasherActor = passwordHasherActor;
    }

    @Override
    public Route createRoutes() {
        return concat(
                pathPrefix("register", () -> concat(
                        pathEnd(() -> post(() -> entity(Jackson.unmarshaller(CreateUserRequest.class), user -> {
                            ActorRef<CreateUserFlow.Command> createUserFlow = system.systemActorOf(
                                    CreateUserFlow.create(passwordHasherActor, unitsUserActor),
                                    "create-flow-" + UUID.randomUUID(),
                                    Props.empty()
                            );

                            CompletionStage<HttpFinalResponse> responseFuture = AskPattern.ask(
                                    createUserFlow,
                                    replyTo -> new CreateUserFlow.StartFlow(user, replyTo),
                                    Duration.ofSeconds(5),
                                    system.scheduler()
                            );

                            return onSuccess(responseFuture, this::completeHttp);
                        })))
                )),
                pathPrefix("login", () -> post(() -> entity(Jackson.unmarshaller(LoginRequest.class), loginRequest -> {
                    CompletionStage<AuthTokenGeneratorMessages.TokenResponse> tokenResponse = AskPattern.ask(
                            tokenGeneratorActor,
                            replyTo -> new AuthTokenGeneratorMessages.GenerateToken(loginRequest.email(), loginRequest.password(), replyTo),
                            Duration.ofSeconds(3),
                            system.scheduler()
                    );

                    CompletionStage<HttpFinalResponse> finalResponse = tokenResponse.thenApply(response -> {
                        if(response instanceof AuthTokenGeneratorMessages.TokenGenerateSuccess success) {
                            return new HttpFinalResponse(200, mapper.valueToTree(success));
                        } else {
                            return new HttpFinalResponse(401, mapper.createObjectNode().put("error", "Invalid credentials"));
                        }
                    });

                    return onSuccess(finalResponse, this::completeHttp);
                }))),
                pathPrefix("me", () -> post(() -> authenticated(user -> {
                    CompletionStage<UserCrudProtocol.UserResponse> dbResponse = AskPattern.ask(
                            unitsUserActor,
                            replyTo -> new UserCrudProtocol.GetUser_Crud_Email(user.email, replyTo),
                            Duration.ofSeconds(3),
                            system.scheduler()
                    );

                    CompletionStage<HttpFinalResponse> finalResponse = dbResponse.thenApply(response -> {
                        if (response instanceof UserCrudProtocol.UserFound found) {
                            return new HttpFinalResponse(200, mapper.valueToTree(found));
                        } else {
                            return new HttpFinalResponse(404, mapper.createObjectNode().put("error", "Profile not found"));
                        }
                    });

                    return onSuccess(finalResponse, this::completeHttp);

                }))),
                pathPrefix("users", () -> concat(
                        path(uuidSegment(), userId -> get(() -> authenticated(user -> {
                            CompletionStage<UserCrudProtocol.UserResponse> dbResponse = AskPattern.ask(
                                    unitsUserActor,
                                    replyTo -> new UserCrudProtocol.GetUserCrud(userId, replyTo),
                                    Duration.ofSeconds(3),
                                    system.scheduler()
                            );

                            CompletionStage<HttpFinalResponse> finalResponse = dbResponse.thenApply(response -> {
                                if (response instanceof UserCrudProtocol.UserFound found) {
                                    return new HttpFinalResponse(200, mapper.valueToTree(found));
                                } else {
                                    return new HttpFinalResponse(404,
                                            mapper.createObjectNode().put("error", "User not found"));
                                }
                            });

                            return onSuccess(finalResponse, this::completeHttp);
                        }))),
                        pathEnd(() -> get(() ->
                                parameterOptional("email", emailOpt -> authenticated(user -> {
                                    String email = emailOpt.orElse(null);
                                    if (email == null) {
                                        return completeHttp(new HttpFinalResponse(400,
                                                mapper.createObjectNode().put("error", "email parameter required")));
                                    }

                                    CompletionStage<UserCrudProtocol.UserResponse> dbResponse = AskPattern.ask(
                                            unitsUserActor,
                                            replyTo -> new UserCrudProtocol.GetUser_Crud_Email(email, replyTo),
                                            Duration.ofSeconds(3),
                                            system.scheduler()
                                    );

                                    CompletionStage<HttpFinalResponse> finalResponse = dbResponse.thenApply(response -> {
                                        if (response instanceof UserCrudProtocol.UserFound found) {
                                            return new HttpFinalResponse(200, mapper.valueToTree(found));
                                        } else {
                                            return new HttpFinalResponse(404,
                                                    mapper.createObjectNode().put("error", "User not found"));
                                        }
                                    });

                                    return onSuccess(finalResponse, this::completeHttp);
                                }))
                        ))
                ))
        );
    }

}
