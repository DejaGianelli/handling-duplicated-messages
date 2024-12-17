package com.drivelab.handling.duplicated.messages.messaging;

import java.util.UUID;

public class DuplicatedMessageException extends RuntimeException {
    private final UUID messageId;

    public DuplicatedMessageException(UUID messageId) {
        this.messageId = messageId;
    }

    public UUID getMessageId() {
        return messageId;
    }
}
