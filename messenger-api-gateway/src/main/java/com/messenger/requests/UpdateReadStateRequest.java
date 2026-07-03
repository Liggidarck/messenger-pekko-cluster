package com.messenger.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.messenger.messages.CborSerializable;

import java.util.UUID;

public record UpdateReadStateRequest(
        UUID lastReadMessageId
) implements CborSerializable {

    @JsonCreator
    public UpdateReadStateRequest(
            @JsonProperty("lastReadMessageId") UUID lastReadMessageId) {
        this.lastReadMessageId = lastReadMessageId;
    }
}
