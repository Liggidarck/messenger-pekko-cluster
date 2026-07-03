package com.messenger.actor;

import com.messenger.cassandra.CassandraSessionHolder;
import com.messenger.messages.core.ChannelEntityProtocol;
import com.messenger.messages.core.ServerEntityProtocol;
import com.messenger.messages.core.UserEntityProtocol;
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

    public SystemGuardian(ActorContext<Void> context) {
        super(context);

        PekkoManagement.get(context.getSystem()).start();
        ClusterBootstrap.get(context.getSystem()).start();

        CassandraSessionHolder.init();

        ClusterSharding sharding = ClusterSharding.get(context.getSystem());
        sharding.init(
                Entity.of(UserEntityProtocol.ENTITY_KEY, UserEntityActor::create)
                        .withRole("chat-core")
        );
        sharding.init(
                Entity.of(ChannelEntityProtocol.ENTITY_KEY, ChannelEntityActor::create)
                        .withRole("chat-core")
        );
        sharding.init(
                Entity.of(ServerEntityProtocol.ENTITY_KEY, ServerEntityActor::create)
                        .withRole("chat-core")
        );
    }

    public static Behavior<Void> create() {
        return Behaviors.setup(SystemGuardian::new);
    }

    @Override
    public Receive<Void> createReceive() {
        return newReceiveBuilder().build();
    }
}
