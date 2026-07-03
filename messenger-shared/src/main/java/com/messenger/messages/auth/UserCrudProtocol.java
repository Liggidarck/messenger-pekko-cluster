package com.messenger.messages.auth;

import com.messenger.messages.CborSerializable;
import com.messenger.messages.registry.ServiceKeyRegistry;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;

import java.util.UUID;

public interface UserCrudProtocol {

    ServiceKey<UserCrudCommand> AUTH_CRUD_USER_KEY = ServiceKey.create(UserCrudCommand.class, ServiceKeyRegistry.AUTH_CRUD_USER_KEY.key);

    record UserView(UUID id, String name, String lastName, String email) implements CborSerializable {
        public UserView(String name, String lastName, String email) {
            this(null, name, lastName, email);
        }
    }

    interface UserCrudCommand extends CborSerializable {}
    sealed interface UserResponse extends CborSerializable permits LoginSuccess, OperationFailed, UserCreated, UserFound, UserNotFound, UserUpdated {}

    record CreateUserCrud(UserView user, String password, ActorRef<UserResponse> replyTo) implements UserCrudCommand {}
    record UserCreated(UserView user) implements UserResponse {}

    record LoginUser(String email, ActorRef<UserResponse> replyTo) implements UserCrudCommand {}
    record LoginSuccess(UserView userView, String hashPassword) implements UserResponse {}

    record GetUserCrud(UUID id, ActorRef<UserResponse> replyTo) implements UserCrudCommand {}
    record GetUser_Crud_Email(String email, ActorRef<UserResponse> replyTo) implements UserCrudCommand {}
    record UserFound(UserView user) implements UserResponse {}

    record UpdateUserCrud(UserView user, ActorRef<UserResponse> replyTo) implements UserCrudCommand {}
    record UserUpdated(UserView user) implements UserResponse {}

    record OperationFailed(String reason) implements UserResponse {}
    record UserNotFound(UUID id) implements UserResponse {}


}
