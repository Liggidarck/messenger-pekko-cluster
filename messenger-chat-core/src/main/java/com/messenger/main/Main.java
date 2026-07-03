package com.messenger.main;

import com.messenger.actor.SystemGuardian;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.typed.ActorSystem;

public class Main {

    private static final Config config = ConfigFactory.load();

    public static void main(String[] args) {
        ActorSystem<Void> system = ActorSystem.create(SystemGuardian.create(), "messengerCluster", config);
        system.getWhenTerminated().toCompletableFuture().join();
    }
}
