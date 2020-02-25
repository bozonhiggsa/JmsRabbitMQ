package net.jms.rabbitmq.application.example4;

import com.rabbitmq.client.*;

public class ReceiveLogsDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // We will use a direct exchange. The routing algorithm behind a direct exchange
        // is simple - a message goes to the queues whose binding key exactly matches
        // the routing key of the message.
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        String queueName = channel.queueDeclare().getQueue();

        if (argv.length < 1) {
            System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
            System.exit(1);
        }

        for (String severity : argv) {
            // A binding is a relationship between an exchange and a queue. This can be simply
            // read as: the queue is interested in messages from this exchange.
            // Bindings can take an extra routingKey parameter - binding key.
            // The meaning of a binding key depends on the exchange type. The fanout exchanges,
            // which we used previously, simply ignored its value.
            channel.queueBind(queueName, EXCHANGE_NAME, severity);
        }
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
