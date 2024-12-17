package com.drivelab.handling.duplicated.messages.config;

import com.drivelab.handling.duplicated.messages.messaging.DuplicatedMessageException;
import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.acknowledgement.AsyncAcknowledgementResultCallback;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import io.awspring.cloud.sqs.listener.errorhandler.ErrorHandler;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@EnableAsync
@EnableScheduling
@Configuration
public class AppConfig {

    @Bean
    SqsMessageListenerContainerFactory<Object> sqsListenerContainerFactory(SqsAsyncClient sqsAsyncClient) {
        return SqsMessageListenerContainerFactory
                .builder()
                .configure(options -> options
                        .acknowledgementMode(AcknowledgementMode.MANUAL)
                )
                .errorHandler(getErrorHandler())
                .sqsAsyncClient(sqsAsyncClient)
                .acknowledgementResultCallback(getAsyncAcknowledgementResultCallback())
                .build();
    }

    private ErrorHandler<Object> getErrorHandler() {
        return new ErrorHandler<>() {
            private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandler.class);

            @Override
            public void handle(Message<Object> message, Throwable t) {
                if (ExceptionUtils.getRootCause(t) instanceof DuplicatedMessageException ex) {
                    LOGGER.warn("Duplicated messaged {} discarded. Trying to acknowledge", ex.getMessageId());
                    return;
                }
                ErrorHandler.super.handle(message, t);
            }
        };
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
