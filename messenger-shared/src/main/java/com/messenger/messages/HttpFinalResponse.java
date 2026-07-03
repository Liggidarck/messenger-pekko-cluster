package com.messenger.messages;

import com.fasterxml.jackson.databind.JsonNode;

public record HttpFinalResponse(int statusCode, JsonNode body) {}

