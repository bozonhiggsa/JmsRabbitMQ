package net.jms.rabbitmq.application.example7;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BooleanSupplier;

/**
 * Publisher confirms are a RabbitMQ extension to implement reliable publishing.
 * When publisher confirms are enabled on a channel, messages the client publishes are confirmed
 * asynchronously by the broker, meaning they have been taken care of on the server side.
 *
 * Strategy #1: Publishing Messages Individually.
 * Publishing a message and waiting synchronously for its confirmation.
 * The method Channel#waitForConfirmsOrDie(long) returns as soon as the message has been confirmed.
 * If the message is not confirmed within the timeout or if it is nack-ed (meaning the broker could not take care
 * of it for some reason), the method will throw an exception. The handling of the exception will usually
 * consists in logging an error message and/or retrying to send the message.
 * This techniques is very straightforward but also has a major drawback: it significantly slows down publishing,
 * as the confirmation of a message blocks the publishing of all subsequent messages.
 * This approach is not going to deliver throughput of more than a few hundreds of published messages per second.
 *
 * In fact, broker confirms published messages asynchronously but in the strategy the code waits synchronously
 * until the message is confirmed. The client actually receives confirms asynchronously and unblocks the call
 * to waitForConfirmsOrDie accordingly.
 * Think of waitForConfirmsOrDie as a synchronous helper which relies on asynchronous notifications under the hood.
 *
 * Strategy #2: Publishing Messages in Batches.
 * Waiting for a batch of messages to be confirmed improves throughput drastically over waiting for a confirm
 * for individual message (up to 20-30 times with a remote RabbitMQ node). One drawback is that we do not know
 * exactly what went wrong in case of failure, so we may have to keep a whole batch in memory
 * to log something meaningful or to re-publish the messages. And this solution is still synchronous,
 * so it blocks the publishing of messages.
 *
 * Strategy #3: Handling Publisher Confirms Asynchronously.
 * Handling publisher confirms asynchronously usually requires the following steps:
 * - provide a way to correlate the publishing sequence number with a message.
 * - register a confirm listener on the channel to be notified when publisher acks/nacks arrive
 * to perform the appropriate actions, like logging or re-publishing a nack-ed message.
 * The sequence-number-to-message correlation mechanism may also require some cleaning during this step.
 * - track the publishing sequence number before publishing a message.
 *
 * This example use a ConcurrentNavigableMap to track outstanding confirms. This data structure is convenient
 * for several reasons. It allows to easily correlate a sequence number with a message
 * (whatever the message data is) and to easily clean the entries up to a give sequence id
 * (to handle multiple confirms/nacks).
 * At last, it supports concurrent access, because confirm callbacks are called in a thread owned
 * by the client library, which should be kept different from the publishing thread.
 * There are other ways to track outstanding confirms than with a sophisticated map implementation,
 * like using a simple concurrent hash map and a variable to track the lower bound of the publishing sequence.
 *
 * Making sure published messages made it to the broker can be essential in some applications.
 * Publisher confirms are a RabbitMQ feature that helps to meet this requirement. Publisher confirms
 * are asynchronous in nature but it is also possible to handle them synchronously. There is no definitive
 * way to implement publisher confirms, this usually comes down to the constraints in the application
 * and in the overall system. Typical techniques are:
 * - publishing messages individually, waiting for the confirmation synchronously:
 * simple, but very limited throughput.
 * - publishing messages in batch, waiting for the confirmation synchronously for a batch:
 * simple, reasonable throughput, but hard to reason about when something goes wrong.
 * - asynchronous handling: best performance and use of resources, good control in case of error,
 * but can be involved to implement correctly.
 *
 */

public class PublisherConfirms {

    static final int MESSAGE_COUNT = 50_000;

    static Connection createConnection() throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost("localhost");
        cf.setUsername("guest");
        cf.setPassword("guest");
        return cf.newConnection();
    }

    public static void main(String[] args) throws Exception {
        publishMessagesIndividually();
        publishMessagesInBatch();
        handlePublishConfirmsAsynchronously();
    }

    static void publishMessagesIndividually() throws Exception {
        try (Connection connection = createConnection()) {
            Channel ch = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            // Publishers confirms are a RabbitMQ extension to the AMQP 0.9.1 protocol, so they
            // are not enabled by default.
            // Publisher confirms are enabled at the channel level with the confirmSelect method
            // This method must be called on every channel that you expect to use publisher confirms.
            // Confirms should be enabled just once, not for every message published
            ch.confirmSelect();
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                ch.basicPublish("", queue, null, body.getBytes());
                ch.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT,
                    Duration.ofNanos(end - start).toMillis());
        }
    }

    static void publishMessagesInBatch() throws Exception {
        try (Connection connection = createConnection()) {
            Channel ch = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            ch.confirmSelect();

            int batchSize = 100;
            int outstandingMessageCount = 0;

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                ch.basicPublish("", queue, null, body.getBytes());
                outstandingMessageCount++;

                if (outstandingMessageCount == batchSize) {
                    ch.waitForConfirmsOrDie(5_000);
                    outstandingMessageCount = 0;
                }
            }

            if (outstandingMessageCount > 0) {
                ch.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT,
                    Duration.ofNanos(end - start).toMillis());
        }
    }

    static void handlePublishConfirmsAsynchronously() throws Exception {
        try (Connection connection = createConnection()) {
            Channel ch = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            ch.confirmSelect();

            // A simple way to correlate messages with sequence number consists in using a map.
            // The publishing code now tracks outbound messages with a map. We need to clean this map
            // when confirms arrive and do something like logging a warning when messages are nack-ed.
            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

            // There are 2 callbacks: one for confirmed messages and one for nack-ed messages
            // (messages that can be considered lost by the broker). Each callback has 2 parameters:
            // - sequence number: a number that identifies the confirmed or nack-ed message.
            // - multiple: this is a boolean value. If false, only one message is confirmed/nack-ed,
            // if true, all messages with a lower or equal sequence number are confirmed/nack-ed.
            // Callback cleans the map when confirms arrive. Note this callback handles both single and multiple
            // confirms. This callback is used when confirms arrive (as the first argument of
            // Channel#addConfirmListener). The callback for nack-ed messages retrieves the message body
            // and issue a warning. It then re-uses the previous callback to clean the map of outstanding
            // confirms (whether messages are confirmed or nack-ed, their corresponding entries
            // in the map must be removed.)
                    ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
                if (multiple) {
                    ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(
                            sequenceNumber, true
                    );
                    confirmed.clear();
                } else {
                    outstandingConfirms.remove(sequenceNumber);
                }
            };

            // The broker confirms published messages asynchronously, one just needs to register a callback
            // on the client to be notified of these confirms:
            ch.addConfirmListener(cleanOutstandingConfirms, (sequenceNumber, multiple) -> {
                String body = outstandingConfirms.get(sequenceNumber);
                System.err.format(
                        "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                        body, sequenceNumber, multiple
                );
                cleanOutstandingConfirms.handle(sequenceNumber, multiple);
            });

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                // The sequence number can be obtained with Channel#getNextPublishSeqNo() before publishing
                outstandingConfirms.put(ch.getNextPublishSeqNo(), body);
                ch.basicPublish("", queue, null, body.getBytes());
            }

            if (!waitUntil(Duration.ofSeconds(60), () -> outstandingConfirms.isEmpty())) {
                throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
            }

            long end = System.nanoTime();
            System.out.format("Published %,d messages and handled confirms asynchronously in %,d ms%n",
                    MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited = +100;
        }
        return condition.getAsBoolean();
    }
}
