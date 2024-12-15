package com.drivelab.handling.duplicated.messages.messaging;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, Integer> {

    @Modifying
    @Query("""
            DELETE FROM ProcessedMessage pm WHERE pm.createdAt <= :date
            """)
    void deleteProcessedMessagesOlderThan(@Param("date") LocalDateTime date);
}
