package net.jms.rabbitmq.application;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Worker {
    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        // When RabbitMQ quits or crashes it will forget the queues and messages unless you tell it not to.
        // Two things are required to make sure that messages aren't lost: we need to mark both the queue
        // and messages as durable
        boolean durable = true;

        channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // This tells RabbitMQ not to give more than one message to a worker at a time. Or, in other words,
        // don't dispatch a new message to a worker until it has processed and acknowledged the previous one.
        // Instead, it will dispatch it to the next worker that is not still busy.
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            System.out.println(" [x] Received " + message + "");
            try {
                doWork(message);
            } finally {
                System.out.println(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        // Message acknowledgment.
        // If a consumer dies (its channel is closed, connection is closed, or TCP connection is lost) without
        // sending an ack, RabbitMQ will understand that a message wasn't processed fully and will re-queue it.
        // If there are other consumers online at the same time, it will then quickly redeliver it to another consumer.
        // autoAck = false - turn on
        // true if the server should consider messages acknowledged once delivered;
        // false if the server should expect explicit acknowledgements
        boolean autoAck = false;
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
