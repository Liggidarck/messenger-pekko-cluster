package com.messenger.messages.registry;

public enum ServiceKeyRegistry {
    HASH_PASSWORD("hash-password"),
    AUTH_VALIDATOR("auth-validator"),
    AUTH_TOKEN_GENERATOR("auth-token-generator"),
    AUTH_CRUD_USER_KEY("auth-crud-user");

    public final String key;
    ServiceKeyRegistry(String key) {
        this.key = key;
    }
}
