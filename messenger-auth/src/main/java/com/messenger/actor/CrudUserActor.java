package com.messenger.actor;

import com.messenger.messages.auth.UserCrudProtocol;
import com.messenger.models.User;
import com.messenger.repository.UserRepository;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.DispatcherSelector;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.receptionist.Receptionist;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class CrudUserActor extends AbstractBehavior<UserCrudProtocol.UserCrudCommand> {

    private final UserRepository userRepository;
    private final Executor jdbcExecutor;

    public CrudUserActor(ActorContext<UserCrudProtocol.UserCrudCommand> context, UserRepository userRepository) {
        super(context);

        context.getSystem().receptionist().tell(Receptionist.register(UserCrudProtocol.AUTH_CRUD_USER_KEY, context.getSelf()));
        this.userRepository = userRepository;
        this.jdbcExecutor = context.getSystem().dispatchers().lookup(DispatcherSelector.fromConfig("jdbc-dispatcher"));
    }

    public static Behavior<UserCrudProtocol.UserCrudCommand> create(UserRepository userRepository) {
        return Behaviors.setup(context -> new CrudUserActor(context, userRepository));
    }


    private record InternalDbResponse(UserCrudProtocol.UserResponse result, ActorRef<UserCrudProtocol.UserResponse> replyTo) implements UserCrudProtocol.UserCrudCommand {}

    @Override
    public Receive<UserCrudProtocol.UserCrudCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(UserCrudProtocol.LoginUser.class, this::onLogin)
                .onMessage(UserCrudProtocol.CreateUserCrud.class, this::onCreateUser)
                .onMessage(UserCrudProtocol.GetUserCrud.class, this::onGetUser)
                .onMessage(UserCrudProtocol.GetUser_Crud_Email.class, this::onGetUser_Email)
                .onMessage(UserCrudProtocol.UpdateUserCrud.class, this::onUpdateUser)
                .onMessage(InternalDbResponse.class, this::onInternalDbResponse)
                .build();
    }

    private void executeAndPipe(CompletableFuture<UserCrudProtocol.UserResponse> future, ActorRef<UserCrudProtocol.UserResponse> replyTo) {
        getContext().pipeToSelf(future, (result, exception) -> {
            if (exception != null) {
                return new InternalDbResponse(new UserCrudProtocol.OperationFailed(exception.getMessage()), replyTo);
            }
            return new InternalDbResponse(result, replyTo);
        });
    }

    private Behavior<UserCrudProtocol.UserCrudCommand> onCreateUser(UserCrudProtocol.CreateUserCrud message) {
        CompletableFuture<UserCrudProtocol.UserResponse> future = CompletableFuture.supplyAsync(() -> {
            User user = new User(UUID.randomUUID(), message.user().name(), message.user().lastName(), message.user().email(), message.password());
            User saved = userRepository.save(user);
            return new UserCrudProtocol.UserCreated(new UserCrudProtocol.UserView(saved.getId(), saved.getName(), saved.getLastName(), saved.getEmail()));
        }, jdbcExecutor);

        executeAndPipe(future, message.replyTo());
        return this;
    }

    private Behavior<UserCrudProtocol.UserCrudCommand> onGetUser(UserCrudProtocol.GetUserCrud message) {
        CompletableFuture<UserCrudProtocol.UserResponse> future = CompletableFuture.supplyAsync(() -> {
            User found = userRepository.findById(message.id());
            if (found != null) {
                return new UserCrudProtocol.UserFound(new UserCrudProtocol.UserView(found.getId(), found.getName(), found.getLastName(), found.getEmail()));
            } else {
                return new UserCrudProtocol.UserNotFound(message.id());
            }
        }, jdbcExecutor);

        executeAndPipe(future, message.replyTo());
        return this;
    }

    private Behavior<UserCrudProtocol.UserCrudCommand> onLogin(UserCrudProtocol.LoginUser loginUser) {
        CompletableFuture<UserCrudProtocol.UserResponse> future = CompletableFuture.supplyAsync(() -> {
            User found = userRepository.findByEmail(loginUser.email());
            if (found != null) {
                return new UserCrudProtocol.LoginSuccess(new UserCrudProtocol.UserView(found.getId(), found.getName(), found.getLastName(), found.getEmail()), found.getHashPassword());
            } else {
                return new UserCrudProtocol.UserNotFound(null);
            }
        }, jdbcExecutor);

        executeAndPipe(future, loginUser.replyTo());
        return this;
    }

    private Behavior<UserCrudProtocol.UserCrudCommand> onGetUser_Email(UserCrudProtocol.GetUser_Crud_Email message) {
        CompletableFuture<UserCrudProtocol.UserResponse> future = CompletableFuture.supplyAsync(() -> {
            User found = userRepository.findByEmail(message.email());
            if (found != null) {
                return new UserCrudProtocol.UserFound(new UserCrudProtocol.UserView(found.getId(), found.getName(), found.getLastName(), found.getEmail()));
            } else {
                return new UserCrudProtocol.UserNotFound(null);
            }
        }, jdbcExecutor);

        executeAndPipe(future, message.replyTo());
        return this;
    }

    private Behavior<UserCrudProtocol.UserCrudCommand> onUpdateUser(UserCrudProtocol.UpdateUserCrud message) {
        CompletableFuture<UserCrudProtocol.UserResponse> future = CompletableFuture.supplyAsync(() -> {
            User existing = userRepository.findById(message.user().id());
            if (existing == null) {
                return new UserCrudProtocol.UserNotFound(message.user().id());
            }

            existing.setName(message.user().name());
            existing.setLastName(message.user().lastName());
            existing.setEmail(message.user().email());

            User updated = userRepository.update(existing);
            return new UserCrudProtocol.UserUpdated(new UserCrudProtocol.UserView(updated.getId(), updated.getName(), updated.getLastName(), updated.getEmail()));
        }, jdbcExecutor);

        executeAndPipe(future, message.replyTo());
        return this;
    }

    private Behavior<UserCrudProtocol.UserCrudCommand> onInternalDbResponse(InternalDbResponse message) {
        message.replyTo.tell(message.result);
        return this;
    }
}
