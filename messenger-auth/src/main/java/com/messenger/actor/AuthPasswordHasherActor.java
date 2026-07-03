
package com.messenger.actor;

import com.messenger.messages.auth.AuthPasswordHashMessages;
import com.messenger.utils.PasswordUtils;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.receptionist.Receptionist;

public class AuthPasswordHasherActor extends AbstractBehavior<AuthPasswordHashMessages.StartHashPasswd> {

    public AuthPasswordHasherActor(ActorContext<AuthPasswordHashMessages.StartHashPasswd> context) {
        super(context);
        context.getSystem().receptionist().tell(
                Receptionist.register(AuthPasswordHashMessages.AUTH_HASH_SERVICE_KEY, context.getSelf())
        );

    }

    public static Behavior<AuthPasswordHashMessages.StartHashPasswd> create() {
        return Behaviors.setup(AuthPasswordHasherActor::new);
    }

    @Override
    public Receive<AuthPasswordHashMessages.StartHashPasswd> createReceive() {
        return newReceiveBuilder()
                .onMessage(AuthPasswordHashMessages.StartHashPasswd.class, this::startHash)
                .build();
    }

    private Behavior<AuthPasswordHashMessages.StartHashPasswd> startHash(AuthPasswordHashMessages.StartHashPasswd startHashPasswd) {
        String hashedPassword = PasswordUtils.hashPassword(startHashPasswd.password());

        startHashPasswd.replyTo().tell(new AuthPasswordHashMessages.HashedPassword(startHashPasswd.password(), hashedPassword));
        return this;
    }
}
