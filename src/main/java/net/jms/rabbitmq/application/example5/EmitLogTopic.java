package net.jms.rabbitmq.application.example5;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLogTopic {

    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // topic exchange
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            //Messages sent to a topic exchange can't have an arbitrary routing_key - it must be
            // a list of words, delimited by dots. The words can be anything, but usually they
            // specify some features connected to the message. A few valid routing key examples:
            // "stock.usd.nyse", "nyse.vmw", "quick.orange.rabbit".
            // The logic behind the topic exchange is similar to a direct one - a message sent with
            // a particular routing key will be delivered to all the queues that are bound with
            // a matching binding key. However there are two important special cases for binding keys:
            // * (star) can substitute for exactly one word.
            // # (hash) can substitute for zero or more words.
            // For example, routing key "<speed>.<colour>.<species>" like that:
            // "*.orange.*"
            // "*.*.rabbit"
            // "lazy.#"
            // if we break our contract and send a message with one or four words, like "orange" or
            // "quick.orange.male.rabbit", these messages won't match any bindings and will be lost.
            // On the other hand "lazy.orange.male.rabbit", even though it has four words,
            // will match the last binding "lazy.#" and will be delivered to the corresponding queue.
            String routingKey = getRouting(argv);
            String message = getMessage(argv);

            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
        }
    }

    private static String getRouting(String[] strings) {
        if (strings.length < 1)
            return "anonymous.info";
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
        if (length < startIndex) return "";
        StringBuilder words = new StringBuilder(strings[startIndex]);
        for (int i = startIndex + 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }
}
