package com.messenger.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class CassandraSessionHolder {
    private static final Logger log = LoggerFactory.getLogger(CassandraSessionHolder.class);
    public static final String KEYSPACE = "messenger";

    private static CqlSession session;

    public static synchronized void init() {
        if (session != null) return;

        String contactPoint = System.getenv().getOrDefault("CASSANDRA_CONTACT_POINTS", "127.0.0.1:9042");
        String[] parts = contactPoint.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9042;
        String datacenter = System.getenv().getOrDefault("CASSANDRA_DATACENTER", "datacenter1");

        log.info("Connecting to Cassandra at {}:{} (DC: {})", host, port, datacenter);

        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(datacenter)
                .build();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        createTables();
        log.info("Cassandra session initialized, all read model tables successfully created");
    }

    // todo: add migrations https://github.com/patka/cassandra-migration
    private static void createTables() {
        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".servers (" +
                "server_id uuid PRIMARY KEY," +
                "name text," +
                "owner_id uuid," +
                "icon_url text," +
                "description text," +
                "created_at timestamp" +
                ");"
        );

        session.execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".channels_by_servers (" +
                        "server_id uuid," +
                        "channel_id uuid," +
                        "category_id uuid," +
                        "position int," +
                        "name text," +
                        "type text," +
                        "topic text," +
                        "created_at timestamp," +
                        "PRIMARY KEY (server_id, channel_id)" +
                        ");"
        );

        session.execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".server_members (" +
                        "server_id uuid," +
                        "user_id uuid," +
                        "joined_at timestamp," +
                        "PRIMARY KEY (server_id, user_id)" +
                        ");"
        );

        session.execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".servers_by_user (" +
                        "user_id uuid," +
                        "server_id uuid," +
                        "joined_at timestamp," +
                        "PRIMARY KEY (user_id, server_id)" +
                        ");"
        );


        session.execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".messages (" +
                        "channel_id uuid," +
                        "message_id timeuuid," +
                        "sender_id uuid," +
                        "text text," +
                        "media_urls list<text>," +
                        "reply_to_message_id timeuuid," +
                        "reactions map< text, frozen<set<text>> >," +
                        "is_edited boolean," +
                        "edited_at timestamp," +
                        "is_deleted boolean," +
                        "PRIMARY KEY (channel_id, message_id)" +
                        ") WITH CLUSTERING ORDER BY (message_id DESC);"
        );

        session.execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".channel_read_states (" +
                        "user_id uuid," +
                        "channel_id uuid," +
                        "last_read_message_id timeuuid," +
                        "PRIMARY KEY (user_id, channel_id)" +
                        ");"
        );

    }

    public static CqlSession getSession() {
        if (session == null) {
            throw new IllegalStateException("CassandraSessionHolder not initialized. Call init() first.");
        }
        return session;
    }

    public static synchronized void shutdown() {
        if (session != null) {
            session.close();
            session = null;
        }
    }
}
