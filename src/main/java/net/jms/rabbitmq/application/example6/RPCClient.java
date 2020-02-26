package net.jms.rabbitmq.application.example6;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * RPC system: a client and a scalable RPC server. RPC service that returns Fibonacci numbers.
 *
 * For an RPC request, the Client sends a message with two properties: replyTo, which is set to a anonymous
 * exclusive queue created just for the request, and correlationId, which is set to a unique value
 * for every request.
 *
 * The request is sent to an rpc_queue queue.
 *
 * The client waits for data on the reply queue. When a message appears, it checks the correlationId property.
 * If it matches the value from the request it returns the response to the application.
 *
 * On the client side, the RPC requires sending and receiving only one message. No synchronous calls
 * like queueDeclare are required. As a result the RPC client needs only one network round
 * trip for a single RPC request.
 *
 */

public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            for (int i = 0; i < 45; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")");
                String response = fibonacciRpc.call(i_str);
                System.out.println(" [.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // sends an RPC request and blocks until the answer is received
    public String call(String message) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();

        // In order to receive a response we need to send a 'callback' queue address with the request.
        // We can use the default exclusive queue.
        // replyTo: Commonly used to name a callback queue.
        // correlationId: Useful to correlate RPC responses with requests. It permits to create
        // a single callback queue per client, not per RPC request
        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        // Since our consumer delivery handling is happening in a separate thread, we're going to need something
        // to suspend the main thread before the response arrives. Usage of BlockingQueue is one possible solutions
        // to do so. Here we are creating ArrayBlockingQueue with capacity set to 1 as we need
        // to wait for only one response.
        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        // The consumer is doing a very simple job, for every consumed response message it checks
        // if the correlationId is the one we're looking for. If so, it puts the response to BlockingQueue.
        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });

        // At the same time main thread is waiting for response to take it from BlockingQueue
        String result = response.take();
        channel.basicCancel(ctag);
        return result;
    }

    public void close() throws IOException {
        connection.close();
    }
}

