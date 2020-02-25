package net.jms.rabbitmq.application.example3;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * In our logging system every running copy of the receiver program will get the messages.
 * That way we'll be able to run one receiver and direct the logs to disk; and at the same time
 * we'll be able to run another receiver and see the logs on the screen.
 * Essentially, published log messages are going to be broadcast to all the receivers.
 */

public class ReceiveLogs {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        // In the Java client, when we supply no parameters to queueDeclare() we create a non-durable,
        // exclusive, auto-delete queue with a generated name
        // Firstly, whenever we connect to Rabbit we need a fresh, empty queue. To do this we could create a queue
        // with a random name, or, even better - let the server choose a random queue name for us.
        // Secondly, once we disconnect the consumer the queue should be automatically deleted.
        String queueName = channel.queueDeclare().getQueue();
        // We've already created a fanout exchange and a queue. Now we need to tell the exchange
        // to send messages to our queue. That relationship between exchange and a queue
        // is called a binding.
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
