package com.colak.loadbalancing;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.nio.charset.StandardCharsets;

public class LoadBalancing {

    private static String NATS_URL = "nats://localhost:4222";
    private static final String SUBJECT = "tasks";

    public static void main() throws Exception {

        // Start two subscribers in a queue group
        new Thread(() -> startSubscriber("Worker-1")).start();
        new Thread(() -> startSubscriber("Worker-2")).start();

        // Give some time for subscribers to start
        Thread.sleep(1_000);

        try (Connection natsConnection = Nats.connect(NATS_URL)) {
            for (int i = 1; i <= 5; i++) {
                String message = "Task " + i;
                natsConnection.publish(SUBJECT, message.getBytes(StandardCharsets.UTF_8));
                System.out.println("Published: " + message);
                Thread.sleep(500);
            }
        }
    }

    private static void startSubscriber(String workerName) {
        try (Connection natsConnection = Nats.connect(NATS_URL)) {
            Dispatcher dispatcher = natsConnection.createDispatcher((msg) -> {
                System.out.println(workerName + " received: " + new String(msg.getData(), StandardCharsets.UTF_8));
            });

            // Subscribe with a queue group named "workers"

            dispatcher.subscribe(SUBJECT, "workers");

            // Keep running
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
