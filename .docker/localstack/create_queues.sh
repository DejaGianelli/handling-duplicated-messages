#!/bin/bash
awslocal sqs create-queue --queue-name OrderCreated
awslocal sqs list-queues