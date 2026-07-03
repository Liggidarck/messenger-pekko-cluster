package com.messenger.actor;

import com.messenger.database.HibernateUtil;
import com.messenger.messages.auth.UserCrudProtocol;
import com.messenger.messages.core.ChannelEntityProtocol;
import com.messenger.messages.core.ServerEntityProtocol;
import com.messenger.messages.core.UserEntityProtocol;
import com.messenger.repository.UserRepository;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.management.cluster.bootstrap.ClusterBootstrap;
import org.apache.pekko.management.javadsl.PekkoManagement;

public class SystemGuardian extends AbstractBehavior<Void> {

    public static Behavior<Void> create() {
        return Behaviors.setup(SystemGuardian::new);
    }

    public SystemGuardian(ActorContext<Void> context) {
        super(context);

        PekkoManagement.get(context.getSystem()).start();
        ClusterBootstrap.get(context.getSystem()).start();

        ClusterSharding sharding = ClusterSharding.get(context.getSystem());
        sharding.init(Entity.of(UserEntityProtocol.ENTITY_KEY, ctxo -> Behaviors.empty())
                .withRole("chat-core"));
        sharding.init(Entity.of(ChannelEntityProtocol.ENTITY_KEY, ctxo -> Behaviors.empty())
                .withRole("chat-core"));
        sharding.init(Entity.of(ServerEntityProtocol.ENTITY_KEY, ctxo -> Behaviors.empty())
                .withRole("chat-core"));

        HibernateUtil.init(context.getSystem());
        UserRepository userRepository = new UserRepository();
        ActorRef<UserCrudProtocol.UserCrudCommand> userActorRef = context.spawn(CrudUserActor.create(userRepository), "auth-user-actor");
        context.spawn(AuthValidatorActor.create(userRepository), "auth-token-validate");
        context.spawn(AuthPasswordHasherActor.create(), "auth-password-generator");
        context.spawn(AuthTokenGeneratorActor.create(userActorRef), "auth-token-generator");

    }

    @Override
    public Receive<Void> createReceive() {
        return newReceiveBuilder().build();
    }
}
