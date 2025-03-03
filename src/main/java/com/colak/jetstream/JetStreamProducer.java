package com.colak.jetstream;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.nio.charset.StandardCharsets;

public class JetStreamProducer {

    public static void main() {
        String natsUrl = "nats://localhost:4222";

        try (Connection natsConnection = Nats.connect(natsUrl)) {
            JetStreamManagement jetStreamManagement = natsConnection.jetStreamManagement();

            // Create a stream (if it doesn't exist)
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .name("ORDERS")
                    .subjects("orders.*")
                    .storageType(StorageType.Memory) // Stores in memory; use File for persistence
                    .build();
            jetStreamManagement.addStream(streamConfiguration);

            // Publish messages
            JetStream jetStream = natsConnection.jetStream();
            for (int i = 1; i <= 5; i++) {
                String message = "Order " + i;
                jetStream.publish("orders.received", message.getBytes(StandardCharsets.UTF_8));
                System.out.println("Published: " + message);
                Thread.sleep(500);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}

