package net.jms.rabbitmq.application.example2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Task {
    private static final String TASK_QUEUE_NAME = "task_queue";
    private static final String CHARSET = "UTF-8";

    public static void main(String[] argv) throws Exception {
        AtomicInteger atomicInteger = new AtomicInteger();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        // When RabbitMQ quits or crashes it will forget the queues and messages unless you tell it not to.
        // Two things are required to make sure that messages aren't lost: we need to mark both the queue
        // and messages as durable
        boolean durable = true;
        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
            channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);

            ScheduledExecutorService executor = Executors.newScheduledThreadPool(5);
            ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
                String message = "task '" + atomicInteger.incrementAndGet() + "'";
                try {
                    // At this point we're sure that the queue won't be lost even if RabbitMQ restarts. Now we need
                    // to mark our messages as persistent - by setting MessageProperties (which implements
                    // BasicProperties) to the value PERSISTENT_TEXT_PLAIN, that corresponds
                    // Content-type "text/plain", deliveryMode=2 (persistent), priority zero
                    channel.basicPublish("", TASK_QUEUE_NAME,
                            MessageProperties.PERSISTENT_TEXT_PLAIN,
                            message.getBytes(CHARSET));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println(" [x] Sent " + message);
            }, 500, 100, TimeUnit.MILLISECONDS);
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
        System.out.println("Overall number of submitted tasks = " + atomicInteger);
    }
}
