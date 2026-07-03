package com.messenger.database;

import com.typesafe.config.Config;
import org.apache.pekko.actor.typed.ActorSystem;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HibernateUtil {
    private static SessionFactory sessionFactory;
    private static final Logger logger = LoggerFactory.getLogger(HibernateUtil.class);

    public static void init(ActorSystem<?> system) {
        if (sessionFactory != null) {
            return;
        }

        try {
            Config config = system.settings().config();
            Config dbConfig = config.getConfig("database.postgres");
            Config hikariConfig = dbConfig.getConfig("hikari");

            Configuration configuration = new Configuration().configure();

            String dbUrl = String.format("jdbc:postgresql://%s:%s/%s",
                    dbConfig.getString("host"),
                    dbConfig.getString("port"),
                    dbConfig.getString("db_name")
            );

            Properties settings = new Properties();
            settings.put("hibernate.connection.url", dbUrl);
            settings.put("hibernate.connection.username", dbConfig.getString("user"));
            settings.put("hibernate.connection.password", dbConfig.getString("password"));
            settings.put("hibernate.hbm2ddl.auto", "validate");

            settings.put("hibernate.connection.provider_class", "org.hibernate.hikaricp.internal.HikariCPConnectionProvider");
            settings.put("hibernate.hikari.idleTimeout", hikariConfig.getString("idleTimeout"));
            settings.put("hibernate.hikari.connectionTimeout", hikariConfig.getString("connectionTimeout"));
            settings.put("hibernate.hikari.maximumPoolSize", hikariConfig.getString("maximumPoolSize"));
            settings.put("hibernate.hikari.minimumIdle", hikariConfig.getString("minimumIdle"));
            settings.put("hibernate.hikari.poolName", hikariConfig.getString("poolName"));
            settings.put("hibernate.hikari.dataSource.reWriteBatchedInserts", "true");

            configuration.addProperties(settings);

            sessionFactory = configuration.buildSessionFactory();
            logger.info("Hibernate initialized successfully. Connected to: " + dbUrl);

        } catch (Throwable ex) {
            logger.error("Initial SessionFactory creation failed." + ex);
            throw new ExceptionInInitializerError(ex);
        }
    }

    public static SessionFactory getSessionFactory() {
        if (sessionFactory == null) {
            throw new IllegalStateException("HibernateUtil is not initialized! Call init(system) first.");
        }
        return sessionFactory;
    }

    public static void shutdown() {
        if (sessionFactory != null) {
            sessionFactory.close();
            logger.info("Hibernate SessionFactory shut down.");
        }
    }
}
