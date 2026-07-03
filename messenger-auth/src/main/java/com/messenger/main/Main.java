package com.messenger.main;

import com.messenger.actor.SystemGuardian;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.typed.ActorSystem;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Config config = ConfigFactory.load();
    private static final Logger logger = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) {
        runPostgresMigrations();
        ActorSystem<?> system = ActorSystem.create(SystemGuardian.create(), "messengerCluster", config);
    }


    private static void runPostgresMigrations() {
        try {
            Config dbConfig = config.getConfig("database.postgres");

            logger.info("Starting database migration for schema 'common'...");
            String dbUrl = String.format("jdbc:postgresql://%s:%s/%s",
                    dbConfig.getString("host"),
                    dbConfig.getString("port"),
                    dbConfig.getString("db_name")
            );

            Flyway flyway = Flyway.configure()
                    .dataSource(dbUrl, dbConfig.getString("user"), dbConfig.getString("password"))
                    .schemas("common")
                    .load();

            flyway.migrate();
            logger.info("Database migration finished successfully.");
        } catch (Exception e) {
            logger.error("postgres migrations failed!", e);
            System.exit(1);
        }
    }

}
