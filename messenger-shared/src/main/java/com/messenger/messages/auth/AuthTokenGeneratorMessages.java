package com.messenger.messages.auth;

import com.messenger.messages.CborSerializable;
import com.messenger.messages.registry.ServiceKeyRegistry;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;

public interface AuthTokenGeneratorMessages {
    ServiceKey<TokenCommand> AUTH_TOKEN_GENERATOR_KEY = ServiceKey.create(TokenCommand.class, ServiceKeyRegistry.AUTH_TOKEN_GENERATOR.key);

    interface TokenCommand extends CborSerializable {}

    sealed interface TokenResponse extends CborSerializable permits TokenGenerateSuccess, TokenGenerateFailed, UpdatedToken {}

    record GenerateToken(String email, String password, ActorRef<TokenResponse> replyTo) implements TokenCommand {}
    record TokenGenerateSuccess(String token, Long expireIn) implements TokenResponse {}
    record TokenGenerateFailed(String reason) implements TokenResponse {}

    record UpdateToken(String token, ActorRef<TokenResponse> replyTo) implements TokenCommand {}
    record UpdatedToken(String newToken, Long expireIn) implements TokenResponse {}
}
