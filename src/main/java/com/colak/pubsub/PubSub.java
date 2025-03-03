package com.colak.pubsub;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;


public class PubSub {

    public static void main() {
        String natsUrl = "nats://localhost:4222"; // Default NATS server address
        String subject = "test-subject";

        try (
                Connection natsConnection = Nats.connect(natsUrl)) {
            // Subscribe to a subject
            Dispatcher dispatcher = natsConnection
                    .createDispatcher((msg) -> System.out.println("Received: " + new String(msg.getData())));

            dispatcher.subscribe(subject);

            // Publish a message
            natsConnection.publish(subject, "Hello, NATS!".getBytes());
            System.out.println("Message sent!");

            // Sleep to allow receiving messages
            Thread.sleep(2000);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

}
