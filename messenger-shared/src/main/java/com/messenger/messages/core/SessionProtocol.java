package com.messenger.messages.core;

public class SessionProtocol {
    public interface SessionCommand {}
    public record IncomingClientMessage(String payload) implements SessionCommand {}
    public record OutgoingClientMessage(String payload) implements SessionCommand {}
    public record ConnectionClosed() implements SessionCommand {}
}
