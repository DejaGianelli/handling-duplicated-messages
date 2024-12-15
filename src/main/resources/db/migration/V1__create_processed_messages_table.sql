CREATE TABLE processed_messages (
    id SERIAL NOT NULL PRIMARY KEY,
    message_id VARCHAR(36) UNIQUE NOT NULL,
    created_at TIMESTAMP NOT NULL
);