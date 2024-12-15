package com.drivelab.handling.duplicated.messages.scheduling;

import com.drivelab.handling.duplicated.messages.messaging.ProcessedMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Component
public class ProcessedMessagesCleaner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessedMessagesCleaner.class);

    private final ProcessedMessageRepository processedMessageRepository;

    @Autowired
    public ProcessedMessagesCleaner(ProcessedMessageRepository processedMessageRepository) {
        this.processedMessageRepository = processedMessageRepository;
    }

    @Transactional
    @Scheduled(fixedDelay = 300000) //5 minutes
    public void clear() {
        LOGGER.info("Cleaning processed messages");
        LocalDateTime date = LocalDateTime.now().minusMinutes(5L);
        processedMessageRepository.deleteProcessedMessagesOlderThan(date);
    }
}
