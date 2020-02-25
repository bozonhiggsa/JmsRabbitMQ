package net.jms.rabbitmq.application.example3;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * After establishing the connection we declared the exchange. This step is necessary as publishing
 * to a non-existing exchange is forbidden.
 * The messages will be lost if no queue is bound to the exchange yet, but that's okay for us;
 * if no consumer is listening yet we can safely discard the message.
 */

public class EmitLog {

    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
            // There are a few exchange types available: direct, topic, headers and fanout.
            // We'll focus on the last one - the fanout. Let's create an exchange of this type,
            // and call it logs:
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            String message = argv.length < 1 ? "info: Hello World!" :
                    String.join(" ", argv);

            // Before we knew nothing about exchanges, but still were able to send messages to queues.
            // That was possible because we were using a default exchange, which we identify by
            // the empty string ("").
            // The first parameter is the name of the exchange. The empty string denotes the default
            // or nameless exchange: messages are routed to the queue with the name specified
            // by routingKey, if it exists.
            // We need to supply a routingKey when sending, but its value is ignored for fanout exchanges.
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
