package com.poc.credit.api.service;

/** Kafka did not acknowledge an event in time. The API fails closed with 503. */
public class PublishFailedException extends RuntimeException {

    public PublishFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
