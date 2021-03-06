package net.jms.rabbitmq.application.example5;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * Although using the direct exchange improved our system, it still has
 * limitations - it can't do routing based on multiple criteria.
 * In our logging system we might want to subscribe to not only logs based on severity,
 * but also based on the source which emitted the log.
 * That would give us a lot of flexibility - we may want to listen to just critical errors
 * coming from 'cron' but also all logs from 'kern'.
 *
 * Topic exchange is powerful and can behave like other exchanges.
 * When a queue is bound with "#" (hash) binding key - it will receive all the messages,
 * regardless of the routing key - like in fanout exchange.
 * When special characters, "*" (star) and "#" (hash), aren't used in bindings, the topic exchange
 * will behave just like a direct one.
 */

public class ReceiveLogsTopic {

    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // topic exchange
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        String queueName = channel.queueDeclare().getQueue();

        if (argv.length < 1) {
            System.err.println("Usage: ReceiveLogsTopic [binding_key]...");
            System.exit(1);
        }

        // One queue may bind with several bindingKeys
        for (String bindingKey : argv) {
            channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
        }

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
