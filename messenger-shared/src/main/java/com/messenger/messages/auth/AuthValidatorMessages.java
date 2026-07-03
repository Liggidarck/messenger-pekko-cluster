package com.messenger.messages.auth;

import com.messenger.messages.CborSerializable;
import com.messenger.messages.registry.ServiceKeyRegistry;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;

import java.util.UUID;

public interface AuthValidatorMessages {
    ServiceKey<ValidateToken> AUTH_VALIDATOR_KEY = ServiceKey.create(ValidateToken.class, ServiceKeyRegistry.AUTH_VALIDATOR.key);

    record ValidateToken(String token, ActorRef<AuthValidatorResult> replyTo) implements CborSerializable {}

    sealed interface AuthValidatorResult extends CborSerializable permits TokenValid, TokenInvalid {}

    record TokenValid(String userEmail, UUID userId) implements AuthValidatorResult {}
    record TokenInvalid(String reason) implements AuthValidatorResult {}
}
