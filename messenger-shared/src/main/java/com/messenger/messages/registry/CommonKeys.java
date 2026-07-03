package com.messenger.messages.registry;

public enum CommonKeys {
    AUTHORIZATION_HEADER("Authorization");

    public final String key;
    CommonKeys(String key) {
        this.key = key;
    }
}
