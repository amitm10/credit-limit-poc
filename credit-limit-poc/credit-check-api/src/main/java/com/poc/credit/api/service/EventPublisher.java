package com.poc.credit.api.service;

import com.poc.credit.api.config.CreditProperties;
import com.poc.credit.common.Json;
import com.poc.credit.common.Topics;
import com.poc.credit.common.events.LimitDefinition;
import com.poc.credit.common.events.ReservationEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes synchronously: the caller waits for the broker's acknowledgement (acks=all,
 * idempotent producer) before it answers, so an APPROVED response always has a durable
 * RESERVED event behind it.
 */
@Component
public class EventPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final CreditProperties props;

    public EventPublisher(KafkaTemplate<String, String> kafka, CreditProperties props) {
        this.kafka = kafka;
        this.props = props;
    }

    public void publish(ReservationEvent event) {
        send(Topics.RESERVATIONS, event.groupId(), Json.write(event));
    }

    public void publish(LimitDefinition definition) {
        send(Topics.LIMITS, definition.key(), Json.write(definition));
    }

    private void send(String topic, String key, String value) {
        try {
            kafka.send(topic, key, value).get(props.kafkaPublishTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishFailedException("Interrupted while publishing to " + topic, e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw new PublishFailedException("Kafka did not acknowledge " + topic + " in time", e);
        }
    }
}
