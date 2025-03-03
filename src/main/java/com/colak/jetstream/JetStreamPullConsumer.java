package com.colak.jetstream;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

// We are using PullSubscribeOptions
public class JetStreamPullConsumer {

    public static void main() {
        String natsUrl = "nats://localhost:4222";

        try (Connection natsConnection = Nats.connect(natsUrl)) {


            // Create a durable consumer
            ConsumerConfiguration consumerConfiguration = ConsumerConfiguration.builder()
                    .durable("order-processor")
                    .ackPolicy(AckPolicy.Explicit) // Require explicit acknowledgment
                    .build();

            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .stream("ORDERS")
                    .configuration(consumerConfiguration)
                    .build();

            // Subscribe to the stream
            JetStream jetStream = natsConnection.jetStream();
            JetStreamSubscription jetStreamSubscription = jetStream.subscribe("orders.received", options);
            natsConnection.flush(Duration.ofSeconds(1));

            // Process messages
            while (true) {
                // Manually Request Messages
                // Fetch up to 10 messages
                List<Message> messages = jetStreamSubscription.fetch(10, Duration.ofSeconds(5));
                for (Message msg : messages) {
                    System.out.println("Processing: " + new String(msg.getData(), StandardCharsets.UTF_8));
                    msg.ack();
                }
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}

