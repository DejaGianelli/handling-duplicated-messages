package com.drivelab.handling.duplicated.messages.listeners;

import com.drivelab.handling.duplicated.messages.domain.services.ProcessOrderCreatedService;
import com.drivelab.handling.duplicated.messages.messaging.DuplicatedMessageException;
import com.drivelab.handling.duplicated.messages.messaging.ProcessedMessage;
import com.drivelab.handling.duplicated.messages.messaging.ProcessedMessageRepository;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
public class OrderCreatedListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderCreatedListener.class);
    private static final String QUEUE_URL = "https://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/OrderCreated";

    private final ProcessedMessageRepository processedMessageRepository;
    private final ProcessOrderCreatedService processOrderCreatedService;

    @Autowired
    public OrderCreatedListener(ProcessedMessageRepository processedMessageRepository,
                                ProcessOrderCreatedService processOrderCreatedService) {
        this.processedMessageRepository = processedMessageRepository;
        this.processOrderCreatedService = processOrderCreatedService;
    }

    @Transactional
    @SqsListener(value = QUEUE_URL, factory = "sqsListenerContainerFactory")
    public void listen(Message<OrderCreatedMessage> message, Acknowledgement ack) {
        UUID messageId = message.getHeaders().getId();

        OrderCreatedMessage orderCreatedMessage = message.getPayload();

        LOGGER.info("Consuming message {}: {}", messageId, message.getPayload());

        processOrderCreatedService.process(orderCreatedMessage.getDishName(), orderCreatedMessage.getPrice());

        this.checkDuplicatedMessage(message, ack);

        LOGGER.info("Message {} processed:", messageId);

        //Comment the line below to emulate a non ACK message
        ack.acknowledgeAsync();
    }

    private void checkDuplicatedMessage(Message<OrderCreatedMessage> message, Acknowledgement ack) {
        UUID messageId = message.getHeaders().getId();
        try {
            ProcessedMessage processedMessage = new ProcessedMessage(messageId);
            processedMessageRepository.save(processedMessage);
        } catch (DataIntegrityViolationException ex) {
            ack.acknowledgeAsync();
            throw new DuplicatedMessageException(messageId);
        }
    }
}
