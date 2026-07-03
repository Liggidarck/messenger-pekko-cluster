package com.messenger.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.messenger.messages.CborSerializable;

import java.util.List;
import java.util.UUID;

public record SendMessageRequest(
        String text,
        List<String> mediaUrls,
        UUID replyToMessageId
) implements CborSerializable {

    @JsonCreator
    public SendMessageRequest(
            @JsonProperty("text") String text,
            @JsonProperty("mediaUrls") List<String> mediaUrls,
            @JsonProperty("replyToMessageId") UUID replyToMessageId) {
        this.text = text;
        this.mediaUrls = mediaUrls;
        this.replyToMessageId = replyToMessageId;
    }
}
