package com.messenger.actor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.messenger.cassandra.CassandraSessionHolder;
import com.messenger.messages.core.ServerEntityProtocol;
import com.messenger.messages.core.UserEntityProtocol;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServerEntityActorTest {

    private static final Config testConfig = ConfigFactory.parseString(
        "pekko.persistence.journal.plugin = \"pekko.persistence.journal.inmem\"\n" +
        "pekko.persistence.snapshot-store.plugin = \"pekko.persistence.snapshot-store.local\""
    ).withFallback(ConfigFactory.load());
    private static final ActorTestKit testKit = ActorTestKit.create(testConfig);
    private static MockedStatic<CassandraSessionHolder> cassandraMock;
    private static CqlSession mockSession;
    private static ClusterSharding mockSharding;

    private final UUID serverId = UUID.randomUUID();
    private final UUID ownerId = UUID.randomUUID();
    private final UUID userId = UUID.randomUUID();
    private ActorRef<ServerEntityProtocol.Command> serverEntity;

    @BeforeAll
    static void setupMocks() {
        mockSharding = mock(ClusterSharding.class, Mockito.RETURNS_DEFAULTS);
        EntityRef<?> noopEntityRef = mock(EntityRef.class, Mockito.RETURNS_DEFAULTS);
        when(mockSharding.entityRefFor(any(), any())).thenReturn((EntityRef) noopEntityRef);

        mockSession = mock(CqlSession.class, Mockito.RETURNS_DEFAULTS);
        when(mockSession.executeAsync(any(String.class), any(Object[].class))).thenReturn(
                CompletableFuture.completedFuture(null));
        when(mockSession.executeAsync(any(String.class))).thenReturn(
                CompletableFuture.completedFuture(mock(AsyncResultSet.class)));

        cassandraMock = Mockito.mockStatic(CassandraSessionHolder.class);
        cassandraMock.when(CassandraSessionHolder::getSession).thenReturn(mockSession);
    }

    @AfterAll
    static void tearDown() {
        cassandraMock.close();
        testKit.shutdownTestKit();
    }

    @BeforeEach
    void setUp() {
        PersistenceId persistenceId = PersistenceId.of("ServerEntity", serverId.toString());
        Behavior<ServerEntityProtocol.Command> behavior = Behaviors.setup(ctx ->
                new ServerEntityActor(persistenceId, ctx, mockSharding, mockSession));
        serverEntity = testKit.spawn(behavior);
    }

    @Test
    void createServerCreatesAndResponds() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);

        serverEntity.tell(new ServerEntityProtocol.CreateServer("test-server", ownerId, probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ServerCreated.class, response);
        assertEquals(serverId, ((ServerEntityProtocol.ServerCreated) response).serverId());
    }

    @Test
    void createDuplicateServerReturnsError() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);
        serverEntity.tell(new ServerEntityProtocol.CreateServer("test", ownerId, probe.ref()));
        probe.receiveMessage();

        serverEntity.tell(new ServerEntityProtocol.CreateServer("dup", ownerId, probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ErrorResponse.class, response);
        assertEquals("ALREADY_EXISTS", ((ServerEntityProtocol.ErrorResponse) response).code());
    }

    @Test
    void addMemberToCreatedServerSucceeds() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);
        serverEntity.tell(new ServerEntityProtocol.CreateServer("test", ownerId, probe.ref()));
        probe.receiveMessage();

        serverEntity.tell(new ServerEntityProtocol.AddMember(userId, probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.MemberAdded.class, response);
    }

    @Test
    void addMemberBeforeServerCreatedReturnsError() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);

        serverEntity.tell(new ServerEntityProtocol.AddMember(userId, probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ErrorResponse.class, response);
        assertEquals("NOT_FOUND", ((ServerEntityProtocol.ErrorResponse) response).code());
    }

    @Test
    void addDuplicateMemberReturnsError() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);
        serverEntity.tell(new ServerEntityProtocol.CreateServer("test", ownerId, probe.ref()));
        probe.receiveMessage();

        serverEntity.tell(new ServerEntityProtocol.AddMember(ownerId, probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ErrorResponse.class, response);
        assertEquals("ALREADY_MEMBER", ((ServerEntityProtocol.ErrorResponse) response).code());
    }

    @Test
    void getServerProfileAfterCreationReturnsProfile() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);
        serverEntity.tell(new ServerEntityProtocol.CreateServer("profile-test", ownerId, probe.ref()));
        probe.receiveMessage();

        serverEntity.tell(new ServerEntityProtocol.GetServerProfile(probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ServerProfile.class, response);
        ServerEntityProtocol.ServerProfile profile = (ServerEntityProtocol.ServerProfile) response;
        assertEquals(serverId, profile.serverId());
        assertEquals("profile-test", profile.name());
        assertEquals(ownerId, profile.ownerId());
        assertEquals(Set.of(ownerId), profile.members());
    }

    @Test
    void getServerProfileBeforeCreationReturnsError() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);

        serverEntity.tell(new ServerEntityProtocol.GetServerProfile(probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ErrorResponse.class, response);
    }

    @Test
    void getMembersAfterAddMemberReturnsAllMembers() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);
        serverEntity.tell(new ServerEntityProtocol.CreateServer("test", ownerId, probe.ref()));
        probe.receiveMessage();
        serverEntity.tell(new ServerEntityProtocol.AddMember(userId, probe.ref()));
        probe.receiveMessage();

        serverEntity.tell(new ServerEntityProtocol.GetMembers(probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.Members.class, response);
        assertEquals(Set.of(ownerId, userId), ((ServerEntityProtocol.Members) response).userIds());
    }

    @Test
    void createChannelAfterServerCreationSucceeds() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);
        serverEntity.tell(new ServerEntityProtocol.CreateServer("test", ownerId, probe.ref()));
        probe.receiveMessage();

        serverEntity.tell(new ServerEntityProtocol.CreateChannel("general", probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ChannelCreated.class, response);
    }

    @Test
    void createChannelBeforeServerCreationReturnsError() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);

        serverEntity.tell(new ServerEntityProtocol.CreateChannel("general", probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ErrorResponse.class, response);
    }

    @Test
    void getChannelsAfterCreatingChannelReturnsChannel() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);
        serverEntity.tell(new ServerEntityProtocol.CreateServer("test", ownerId, probe.ref()));
        probe.receiveMessage();
        serverEntity.tell(new ServerEntityProtocol.CreateChannel("general", probe.ref()));
        probe.receiveMessage();

        serverEntity.tell(new ServerEntityProtocol.GetChannels(probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.Channels.class, response);
        assertEquals(1, ((ServerEntityProtocol.Channels) response).channels().size());
    }

    @Test
    void getChannelsBeforeCreationReturnsError() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);

        serverEntity.tell(new ServerEntityProtocol.GetChannels(probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ErrorResponse.class, response);
    }

    @Test
    void removeMemberRemovesUserFromServer() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);
        serverEntity.tell(new ServerEntityProtocol.CreateServer("test", ownerId, probe.ref()));
        probe.receiveMessage();
        serverEntity.tell(new ServerEntityProtocol.AddMember(userId, probe.ref()));
        probe.receiveMessage();

        serverEntity.tell(new ServerEntityProtocol.RemoveMember(userId, probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.MemberRemoved.class, response);

        serverEntity.tell(new ServerEntityProtocol.GetMembers(probe.ref()));
        response = probe.receiveMessage();
        assertEquals(Set.of(ownerId), ((ServerEntityProtocol.Members) response).userIds());
    }

    @Test
    void removeNonMemberReturnsError() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);
        serverEntity.tell(new ServerEntityProtocol.CreateServer("test", ownerId, probe.ref()));
        probe.receiveMessage();

        serverEntity.tell(new ServerEntityProtocol.RemoveMember(userId, probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ErrorResponse.class, response);
        assertEquals("NOT_A_MEMBER", ((ServerEntityProtocol.ErrorResponse) response).code());
    }

    @Test
    void removeMemberBeforeCreationReturnsError() {
        var probe = testKit.createTestProbe(ServerEntityProtocol.Response.class);

        serverEntity.tell(new ServerEntityProtocol.RemoveMember(userId, probe.ref()));

        ServerEntityProtocol.Response response = probe.receiveMessage();
        assertInstanceOf(ServerEntityProtocol.ErrorResponse.class, response);
        assertEquals("NOT_FOUND", ((ServerEntityProtocol.ErrorResponse) response).code());
    }
}
