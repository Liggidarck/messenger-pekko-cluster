package com.messenger.messages.auth;

import com.messenger.messages.CborSerializable;
import com.messenger.messages.registry.ServiceKeyRegistry;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;

public interface AuthPasswordHashMessages {
    ServiceKey<StartHashPasswd> AUTH_HASH_SERVICE_KEY = ServiceKey.create(StartHashPasswd.class, ServiceKeyRegistry.HASH_PASSWORD.key);

    record StartHashPasswd(String password, ActorRef<HashedPassword> replyTo) implements CborSerializable {}

    record HashedPassword(String originalPassword, String hashPassword) implements CborSerializable {}
}
