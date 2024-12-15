package com.drivelab.handling.duplicated.messages.config;

import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.acknowledgement.AsyncAcknowledgementResultCallback;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Configuration
public class AppConfig {

    @Bean
    SqsMessageListenerContainerFactory<Object> sqsListenerContainerFactory(SqsAsyncClient sqsAsyncClient) {
        return SqsMessageListenerContainerFactory
                .builder()
                .configure(options -> options
                        .acknowledgementMode(AcknowledgementMode.MANUAL)
                )
                .sqsAsyncClient(sqsAsyncClient)
                .acknowledgementResultCallback(getAsyncAcknowledgementResultCallback())
                .build();
    }

    private AsyncAcknowledgementResultCallback<Object> getAsyncAcknowledgementResultCallback() {
        return new AsyncAcknowledgementResultCallback<>() {
            private static final Logger LOGGER = LoggerFactory.getLogger(AsyncAcknowledgementResultCallback.class);

            @Override
            public CompletableFuture<Void> onSuccess(Collection<Message<Object>> messages) {
                for (Message<Object> message : messages) {
                    UUID messageId = message.getHeaders().getId();
                    LOGGER.info("Message {} acknowledged", messageId);
                }
                return AsyncAcknowledgementResultCallback.super.onSuccess(messages);
            }

            @Override
            public CompletableFuture<Void> onFailure(Collection<Message<Object>> messages, Throwable t) {
                for (Message<Object> message : messages) {
                    UUID messageId = message.getHeaders().getId();
                    LOGGER.error("Failed to acknowledge message {}", messageId);
                }
                return AsyncAcknowledgementResultCallback.super.onFailure(messages, t);
            }
        };
    }
}
