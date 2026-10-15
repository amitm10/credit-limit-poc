package com.poc.credit.processor.service;

import com.poc.credit.common.Json;
import com.poc.credit.common.Topics;
import com.poc.credit.common.events.OrderEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes order lifecycle events. Offsets are committed after the listener returns
 * (ack mode BATCH), i.e. after Redis and Kafka both have the change. Infrastructure failures
 * propagate to the error handler, which retries the record until it succeeds.
 */
@Component
public class OrderEventListener {

    public static final String LISTENER_ID = "order-events";

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final EventApplier applier;
    private final MeterRegistry registry;

    public OrderEventListener(EventApplier applier, MeterRegistry registry) {
        this.applier = applier;
        this.registry = registry;
    }

    @KafkaListener(id = LISTENER_ID, groupId = "exposure-processor", topics = Topics.ORDER_EVENTS)
    public void onEvent(ConsumerRecord<String, String> record) {
        OrderEvent event;
        try {
            event = Json.read(record.value(), OrderEvent.class);
            if (event.orderId() == null || event.accountId() == null || event.type() == null) {
                throw new IllegalArgumentException("orderId, accountId and type are required");
            }
        } catch (IllegalArgumentException e) {
            // A malformed event can never succeed: count it and move on rather than block the partition.
            registry.counter("credit.order.events.invalid").increment();
            log.error("Skipping invalid order event at {}-{}@{}: {}", record.topic(), record.partition(),
                    record.offset(), e.getMessage());
            return;
        }
        try {
            applier.apply(event);
        } catch (IllegalArgumentException e) {
            registry.counter("credit.order.events.invalid").increment();
            log.error("Skipping order event with invalid identifiers {}: {}", event, e.getMessage());
        }
    }
}
