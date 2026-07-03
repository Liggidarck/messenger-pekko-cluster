package com.messenger.http;

import com.messenger.messages.auth.AuthPasswordHashMessages;
import com.messenger.messages.auth.AuthTokenGeneratorMessages;
import com.messenger.messages.auth.AuthValidatorMessages;
import com.messenger.messages.auth.UserCrudProtocol;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.http.javadsl.server.Route;

public class MainRouter extends BaseRouter {

    private final ActorSystem<?> system;
    private final ClusterSharding clusterSharding;

    private final ActorRef<AuthValidatorMessages.ValidateToken> validateTokenActor;
    private final ActorRef<AuthPasswordHashMessages.StartHashPasswd> passwordHasherActor;
    private final ActorRef<AuthTokenGeneratorMessages.TokenCommand> tokenGeneratorActor;

    private final ActorRef<UserCrudProtocol.UserCrudCommand> crudUserActor;


    public MainRouter(ActorSystem<?> system,
                      ClusterSharding clusterSharding,
                      ActorRef<AuthValidatorMessages.ValidateToken> validateTokenActor,
                      ActorRef<AuthPasswordHashMessages.StartHashPasswd> passwordHasherActor,
                      ActorRef<AuthTokenGeneratorMessages.TokenCommand> tokenGeneratorActor,
                      ActorRef<UserCrudProtocol.UserCrudCommand> crudUserActor) {
        super(system, validateTokenActor);
        this.system = system;
        this.clusterSharding = clusterSharding;
        this.validateTokenActor = validateTokenActor;
        this.passwordHasherActor = passwordHasherActor;
        this.tokenGeneratorActor = tokenGeneratorActor;
        this.crudUserActor = crudUserActor;
    }

    public Route createRoutes() {
        AuthRouter authRouter = new AuthRouter(system, validateTokenActor, tokenGeneratorActor, passwordHasherActor, crudUserActor);
        WebSocketRouter wsRouter = new WebSocketRouter(system, validateTokenActor, clusterSharding);
        ServerRouter serverRouter = new ServerRouter(system, validateTokenActor, clusterSharding, crudUserActor);
        MessageRouter messageRouter = new MessageRouter(system, validateTokenActor, clusterSharding);

        return concat(
                authRouter.createRoutes(),
                wsRouter.createRoutes(),
                serverRouter.createRoutes(),
                messageRouter.createRoutes()
        );
    }

}
