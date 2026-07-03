package com.messenger.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.messenger.messages.CborSerializable;

public record CreateUserRequest(
        String name,
        String lastName,
        String email,
        String password) implements CborSerializable {

    @JsonCreator
    public CreateUserRequest(
            @JsonProperty("name") String name,
            @JsonProperty("lastName") String lastName,
            @JsonProperty("email") String email,
            @JsonProperty("password") String password) {
        this.name = name;
        this.lastName = lastName;
        this.email = email;
        this.password = password;
    }

}
