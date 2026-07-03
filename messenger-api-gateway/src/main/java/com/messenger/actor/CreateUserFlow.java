package com.messenger.actor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.messenger.messages.CborSerializable;
import com.messenger.messages.HttpFinalResponse;
import com.messenger.messages.auth.AuthPasswordHashMessages;
import com.messenger.messages.auth.UserCrudProtocol;
import com.messenger.requests.CreateUserRequest;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateUserFlow extends AbstractBehavior<CreateUserFlow.Command> {

    private static final Logger logger = LoggerFactory.getLogger(CreateUserFlow.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public interface Command extends CborSerializable {}

    public record StartFlow(CreateUserRequest user, ActorRef<HttpFinalResponse> replyTo) implements Command {}

    private record WrappedHashResult(AuthPasswordHashMessages.HashedPassword res) implements Command {}
    private record WrappedUnitsResult(UserCrudProtocol.UserResponse res) implements Command {}

    private final ActorRef<AuthPasswordHashMessages.StartHashPasswd> passwordHasherActor;
    private final ActorRef<UserCrudProtocol.UserCrudCommand> unitsUserActor;

    private ActorRef<HttpFinalResponse> httpReplyTo;
    private CreateUserRequest user;

    public CreateUserFlow(ActorContext<Command> context, ActorRef<AuthPasswordHashMessages.StartHashPasswd> passwordHasherActor, ActorRef<UserCrudProtocol.UserCrudCommand> unitsUserActor) {
        super(context);
        this.passwordHasherActor = passwordHasherActor;
        this.unitsUserActor = unitsUserActor;
    }

    public static Behavior<Command> create(ActorRef<AuthPasswordHashMessages.StartHashPasswd> passwordHasherActor, ActorRef<UserCrudProtocol.UserCrudCommand> unitsUserActor) {
        return Behaviors.setup(context -> new CreateUserFlow(context, passwordHasherActor, unitsUserActor));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartFlow.class, this::onStart)
                .onMessage(WrappedHashResult.class, this::onHashResponse)
                .onMessage(WrappedUnitsResult.class, this::onUnitsResponse)
                .build();
    }

    private Behavior<Command> onStart(StartFlow startFlow) {
        this.httpReplyTo = startFlow.replyTo;
        this.user = startFlow.user;

        ActorRef<AuthPasswordHashMessages.HashedPassword> adapter =
                getContext().messageAdapter(AuthPasswordHashMessages.HashedPassword.class, WrappedHashResult::new);

        passwordHasherActor.tell(new AuthPasswordHashMessages.StartHashPasswd(startFlow.user.password(), adapter));
        return this;
    }

    private Behavior<Command> onHashResponse(WrappedHashResult wrapper) {
        String hashedPassword = wrapper.res().hashPassword();

        ActorRef<UserCrudProtocol.UserResponse> adapter = getContext().messageAdapter(UserCrudProtocol.UserResponse.class, WrappedUnitsResult::new);

        unitsUserActor.tell(new UserCrudProtocol.CreateUserCrud(new UserCrudProtocol.UserView(user.name(), user.lastName(), user.email()), hashedPassword, adapter));

        return this;
    }

    private Behavior<Command> onUnitsResponse(WrappedUnitsResult wrapper) {
        UserCrudProtocol.UserResponse response = wrapper.res();

        JsonNode body;
        int status;

        switch (response) {
            case UserCrudProtocol.UserCreated created -> {
                body = mapper.valueToTree(created);
                status = 201;
            }
            case UserCrudProtocol.OperationFailed(String reason) -> {
                ObjectNode errorNode = mapper.createObjectNode();
                errorNode.put("error", reason);

                body = errorNode;
                status = 500;
            }
            case UserCrudProtocol.UserNotFound notFound -> {
                body = mapper.valueToTree(notFound);
                status = 404;
            }
            case null, default -> {
                ObjectNode errorNode = mapper.createObjectNode();
                errorNode.put("error", "Unknown response");

                body = errorNode;
                status = 500;
            }
        }

        httpReplyTo.tell(new HttpFinalResponse(status, body));

        return Behaviors.stopped();
    }
}
