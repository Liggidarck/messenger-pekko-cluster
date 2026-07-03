package com.messenger.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.messenger.messages.CborSerializable;

public record CreateChannelRequest(
        String name
) implements CborSerializable {

    @JsonCreator
    public CreateChannelRequest(
            @JsonProperty("name") String name) {
        this.name = name;
    }
}
