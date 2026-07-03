package com.messenger.repository;


import com.messenger.models.User;

import java.util.UUID;

public class UserRepository extends Repository {

    public User findById(UUID id) {
        return executeReadOnly(session -> session.find(User.class, id));
    }

    public User findByEmail(String email) {
        return executeReadOnly(session ->
                session.createQuery("FROM User u WHERE u.email = :email", User.class)
                        .setParameter("email", email)
                        .uniqueResult()
        );
    }

    public User save(User user) {
        return executeInTransaction(session -> {
            session.persist(user);
            return user;
        });
    }

    public User update(User user) {
        return executeInTransaction(session -> session.merge(user));
    }

    public void delete(UUID id) {
        runInTransaction(session -> {
            User ref = session.getReference(User.class, id);
            session.remove(ref);
        });
    }
}
