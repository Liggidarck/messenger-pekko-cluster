package com.messenger.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.messenger.messages.CborSerializable;

import java.util.UUID;

public record AddMemberRequest(
        UUID userId,
        String email
) implements CborSerializable {

    @JsonCreator
    public AddMemberRequest(
            @JsonProperty("userId") UUID userId,
            @JsonProperty("email") String email) {
        this.userId = userId;
        this.email = email;
    }
}
