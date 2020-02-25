package net.jms.rabbitmq.application.example4;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Routing messages to concrete consumers.
 *
 * For example, we will be able to direct only critical error messages to the log file
 * (to save disk space), while still being able to print all of the log messages
 * on the console.
 * We may want a program which writes log messages to the disk to only receive critical errors,
 * and not waste disk space on warning or info log messages.
 */

public class EmitLogDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
            // We will use a direct exchange. The routing algorithm behind a direct exchange
            // is simple - a message goes to the queues whose binding key exactly matches
            // the routing key of the message.
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            String severity = getSeverity(argv);
            String message = getMessage(argv);

            channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
        }
    }

    private static String getSeverity(String[] strings) {
        if (strings.length < 1)
            return "info";
        return strings[0];
    }

    private static String getMessage(String[] strings) {
        if (strings.length < 2)
            return "Hello World!";
        return joinStrings(strings, " ", 1);
    }

    private static String joinStrings(String[] strings, String delimiter, int startIndex) {
        int length = strings.length;
        if (length == 0) return "";
        if (length <= startIndex) return "";
        StringBuilder words = new StringBuilder(strings[startIndex]);
        for (int i = startIndex + 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }
}
