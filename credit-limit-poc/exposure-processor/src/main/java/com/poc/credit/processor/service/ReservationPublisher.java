package com.poc.credit.processor.service;

import com.poc.credit.common.Json;
import com.poc.credit.common.Topics;
import com.poc.credit.common.events.ReservationEvent;
import com.poc.credit.processor.config.ProcessorProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes a reservation state change and waits for the ack. The listener commits the
 * order event's offset only after this returns, so every applied event is reflected in
 * credit.reservations before it is marked consumed.
 */
@Component
public class ReservationPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final ProcessorProperties props;

    public ReservationPublisher(KafkaTemplate<String, String> kafka, ProcessorProperties props) {
        this.kafka = kafka;
        this.props = props;
    }

    public void publish(ReservationEvent event) {
        try {
            kafka.send(Topics.RESERVATIONS, event.groupId(), Json.write(event))
                    .get(props.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Kafka did not acknowledge the reservation event", e);
        }
    }
}
