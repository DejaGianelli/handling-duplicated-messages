# Handling Duplicate Messages

This project demonstrates how to handle duplicate messages in a consumer application using Spring Boot and AWS SQS as the message broker.

When dealing with distributed applications, different parts of the system often exchange messages asynchronously via a message broker. In this case, the broker of choice is **AWS SQS**.

Most message brokers, including SQS, do not guarantee that the same message will be delivered **only once**. Instead, they promise to deliver messages **at least once**. This can result in duplicate messages being sent to consumers.

## Scenarios Leading to Duplicate Messages

Here are some common scenarios where duplicate messages may occur:

1. **Message Not Acknowledged**:
    - Due to issues like network failures or the consumer taking too long to process a message, the broker may not receive an acknowledgment (ACK). As a result, the broker assumes the message was not processed and redelivers it.

2. **Parallel Consumers**:
    - When multiple consumers are processing messages from the same queue, one consumer may pick up a message while another consumer simultaneously receives it. This can happen due to a brief overlap in the visibility timeout or other timing issues.

3. **Producer Sending the Same Message Multiple Times**:
    - If the producer retries sending a message due to a timeout or network issue, and SQS does not recognize it as a duplicate, it will accept and enqueue the duplicate message.

## Mitigation Strategies

To handle duplicate messages, you can adopt one of two primary strategies:

### 1. **Idempotent Message Handlers**
- An idempotent handler processes a message in such a way that even if the same message is processed multiple times, the result remains consistent. However, implementing idempotency can be complex, especially when the application logic involves side effects (e.g., updating external systems, triggering notifications) that must not happen more than once.

### 2. **Discarding Duplicates by Tracking Processed Messages**
- A simpler and often more practical approach is to discard duplicate messages by tracking already processed ones. This can be achieved by storing the IDs of processed messages and discarding any new message with the same ID. This approach ensures that duplicate messages do not trigger redundant processing.

- When using this strategy, you must remember to clean the database table that keeps track of the processed messages. 
Depending on the database that you choose to use, you configure an TTL or a simple scheduler that cleans the old tracked
messages, otherwise this table tends to grow very fast causing an impact on performance. The purpose of this kind of table
is to be ephemeral, just to keep track of the messages for a short period of time (enough to make sure that the message has 
already been handled in any way and won't be lost).  

By applying these strategies, you can ensure that your application remains robust and can handle duplicate messages effectively, maintaining the integrity of your system.


## Example Scenario Implemented

This project serves as a proof of concept by emulating a Restaurant Application. Imagine a scenario where a service responsible for the Kitchen receives events for orders placed by customers. When the Kitchen service receives an order, it must create a ticket so that kitchen employees can begin preparing the dish.

However, the Kitchen must handle possible duplicate Order Created messages to avoid issues. Without proper handling, duplicate messages could result in multiple tickets being created for the same order, leading to unnecessary work, wasted resources, and financial losses for the restaurant.

By implementing strategies to manage duplicate messages, such as idempotent handlers or tracking processed message IDs, the Kitchen service can ensure that each order results in exactly one ticket, maintaining efficiency and preventing losses.

## Running the Project

To get the infrastructure up and running using docker (localstack and postgres), you can simply run:

```shell
docker compose up -d
```

To run the Service:

```shell
mvn spring-boot:run -f pom.xml
```

## Producing Event

To produce order created messages to the queue. Attach to localstack container and run:

```shell
awslocal sqs send-message --queue-url https://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/OrderCreated --message-body '{"dishName":"Parmegiana","price":45.00}'
```