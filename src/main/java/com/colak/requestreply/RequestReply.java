package com.colak.requestreply;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class RequestReply {

    public static void main() {
        String natsUrl = "nats://localhost:4222";
        String subject = "request-subject";

        try (Connection natsConnection = Nats.connect(natsUrl)) {
            // Replying to a request
            Dispatcher dispatcher = natsConnection.createDispatcher((msg) -> {
                String request = new String(msg.getData(), StandardCharsets.UTF_8);
                System.out.println("Received request: " + request);

                // Send reply to the requestor
                natsConnection.publish(msg.getReplyTo(), ("Reply to: " + request).getBytes(StandardCharsets.UTF_8));
            });

            dispatcher.subscribe(subject);

            // Sending a request and waiting for a reply
            Message reply = natsConnection.request(subject,
                    "Hello, are you there?".getBytes(StandardCharsets.UTF_8),
                    Duration.ofSeconds(2));

            if (reply != null) {
                System.out.println("Received reply: " + new String(reply.getData(), StandardCharsets.UTF_8));
            } else {
                System.out.println("No reply received");
            }

            // Sleep to allow time for message processing
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
