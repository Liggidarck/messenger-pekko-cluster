package com.messenger.actor;

import com.messenger.http.MainRouter;
import com.messenger.messages.auth.AuthPasswordHashMessages;
import com.messenger.messages.auth.AuthTokenGeneratorMessages;
import com.messenger.messages.auth.AuthValidatorMessages;
import com.messenger.messages.auth.UserCrudProtocol;
import com.messenger.messages.core.ChannelEntityProtocol;
import com.messenger.messages.core.ServerEntityProtocol;
import com.messenger.messages.core.UserEntityProtocol;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.server.Route;
import org.apache.pekko.management.cluster.bootstrap.ClusterBootstrap;
import org.apache.pekko.management.javadsl.PekkoManagement;

public class SystemGuardian extends AbstractBehavior<Void> {
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

        GroupRouter<AuthValidatorMessages.ValidateToken> authValidatorGroup = Routers.group(AuthValidatorMessages.AUTH_VALIDATOR_KEY);
        ActorRef<AuthValidatorMessages.ValidateToken> validateTokenActor = context.spawn(authValidatorGroup, "auth-token-validate");

        GroupRouter<AuthPasswordHashMessages.StartHashPasswd> passwordHasherGroup = Routers.group(AuthPasswordHashMessages.AUTH_HASH_SERVICE_KEY);
        ActorRef<AuthPasswordHashMessages.StartHashPasswd> passwordHasherActor = context.spawn(passwordHasherGroup, "auth-password-generator");

        GroupRouter<AuthTokenGeneratorMessages.TokenCommand> tokenGeneratorGroup = Routers.group(AuthTokenGeneratorMessages.AUTH_TOKEN_GENERATOR_KEY);
        ActorRef<AuthTokenGeneratorMessages.TokenCommand> tokenGeneratorActor = context.spawn(tokenGeneratorGroup, "auth-token-generator");

        GroupRouter<UserCrudProtocol.UserCrudCommand> userCrudAuthGroup = Routers.group(UserCrudProtocol.AUTH_CRUD_USER_KEY);
        ActorRef<UserCrudProtocol.UserCrudCommand> authCrudUserActor = context.spawn(userCrudAuthGroup, "auth-user-actor");

        MainRouter mainRouter = new MainRouter(context.getSystem(),
                sharding,
                validateTokenActor,
                passwordHasherActor,
                tokenGeneratorActor,
                authCrudUserActor);
        Route routes = mainRouter.createRoutes();

        Http.get(context.getSystem())
                .newServerAt("0.0.0.0", 8080)
                .bind(routes);
    }

    public static Behavior<Void> create() {
        return Behaviors.setup(SystemGuardian::new);
    }

    @Override
    public Receive<Void> createReceive() {
        return newReceiveBuilder().build();
    }
}
