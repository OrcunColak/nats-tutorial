package com.colak.jetstream;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

// We are using PushSubscribeOptions
public class JetStreamPushConsumer {

    public static void main() throws Exception {
        String natsUrl = "nats://localhost:4222";

        try (Connection natsConnection = Nats.connect(natsUrl)) {

            ensureStreamExists(natsConnection);

            // Create a durable consumer configuration
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .durable("order-processor-push")  // NEW consumer name
                    .deliverPolicy(DeliverPolicy.All) // Start from the first message
                    .ackPolicy(AckPolicy.Explicit) // Requires manual acknowledgment
                    .build();

            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .stream("ORDERS")
                    .configuration(consumerConfig)
                    .build();

            // **Subscribe to the JetStream push-based consumer**
            JetStream jetStream = natsConnection.jetStream();
            JetStreamSubscription jetStreamSubscription = jetStream.subscribe("orders.received", options);
            System.out.println("JetStream consumer subscribed, waiting for messages...");

            while (true) {
                Message msg = jetStreamSubscription.nextMessage(Duration.ofSeconds(5)); // Wait for messages
                if (msg != null) {
                    System.out.println("Received: " + new String(msg.getData(), StandardCharsets.UTF_8));
                    msg.ack(); // Explicitly acknowledge
                }
            }
        }
    }

    // Ensure the stream exists
    private static void ensureStreamExists(Connection natsConnection) throws IOException, JetStreamApiException {
        JetStreamManagement jetStreamManagement = natsConnection.jetStreamManagement();
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name("ORDERS")
                .subjects("orders.*")
                .storageType(StorageType.Memory) // Use FILE for persistence
                .build();
        jetStreamManagement.addStream(streamConfig);
    }
}



