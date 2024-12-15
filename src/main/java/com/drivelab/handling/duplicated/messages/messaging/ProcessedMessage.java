package com.drivelab.handling.duplicated.messages.messaging;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "processed_messages")
@Getter
@Setter
public class ProcessedMessage {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private UUID messageId;

    private LocalDateTime createdAt;

    public ProcessedMessage(UUID messageId) {
        this();
        this.messageId = messageId;
    }

    public ProcessedMessage() {
        this.createdAt = LocalDateTime.now();
    }
}
