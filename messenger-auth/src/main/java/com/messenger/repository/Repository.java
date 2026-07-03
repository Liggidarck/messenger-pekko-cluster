package com.messenger.repository;

import com.messenger.database.HibernateUtil;
import jakarta.persistence.PersistenceException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.function.Function;

public abstract class Repository {
    private static final Logger logger = LoggerFactory.getLogger(Repository.class);

    protected <T> T executeInTransaction(Function<Session, T> action) {
        Transaction transaction = null;
        try (Session session = HibernateUtil.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            T result = action.apply(session);
            transaction.commit();
            return result;
        } catch (Exception e) {
            rollback(transaction);
            handleException(e);
            return null;
        }
    }

    protected void runInTransaction(Consumer<Session> action) {
        executeInTransaction(session -> {
            action.accept(session);
            return null;
        });
    }

    protected <T> T executeReadOnly(Function<Session, T> action) {
        Transaction transaction = null;
        try (Session session = HibernateUtil.getSessionFactory().openSession()) {
            session.setDefaultReadOnly(true);

            transaction = session.beginTransaction();
            T result = action.apply(session);
            transaction.commit();
            return result;
        } catch (Exception e) {
            rollback(transaction);
            handleException(e);
            return null;
        }
    }

    private void rollback(Transaction transaction) {
        if (transaction != null && transaction.isActive()) {
            try {
                transaction.rollback();
            } catch (Exception e) {
                logger.error("Failed to rollback transaction", e);
            }
        }
    }

    private void handleException(Exception e) {
        logger.error("Database operation failed", e);
        if (e instanceof PersistenceException) {
            throw new RuntimeException("Data integrity violation: " + e.getMessage(), e);
        }
        throw new RuntimeException("Database error: " + e.getMessage(), e);
    }
}
