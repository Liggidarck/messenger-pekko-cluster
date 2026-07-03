package com.messenger.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.messenger.messages.CborSerializable;

public record EditMessageRequest(
        String text
) implements CborSerializable {

    @JsonCreator
    public EditMessageRequest(
            @JsonProperty("text") String text) {
        this.text = text;
    }
}
