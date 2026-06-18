package com.learn.flink.producer;

import com.learn.flink.common.JsonUtil;
import com.learn.flink.model.Transaction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Standalone Kafka producer (a plain Java main, NOT a Flink job).
 * Run it with:  java -cp target/flink-learning-1.0.jar com.learn.flink.producer.TransactionProducer
 *
 * It continuously publishes JSON transactions to the "transactions" topic, keyed by userId.
 * About 10% of events are emitted with an event-time 20s in the PAST, so that Lab 4
 * (late data + side outputs) has something interesting to catch.
 */
public class TransactionProducer {

    private static final String[] USERS = {"alice", "bob", "carol", "dave", "erin"};
    private static final String[] CATEGORIES = {"grocery", "travel", "electronics", "dining", "fuel"};

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Random random = new Random();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producing transactions to 'transactions' topic. Press Ctrl+C to stop.");
            long sent = 0;
            while (true) {
                Transaction txn = new Transaction();
                txn.txnId = UUID.randomUUID().toString();
                txn.userId = USERS[random.nextInt(USERS.length)];
                txn.category = CATEGORIES[random.nextInt(CATEGORIES.length)];
                txn.amount = Math.round(random.nextDouble() * 50000) / 100.0; // 0.00 - 500.00

                long now = System.currentTimeMillis();
                boolean late = random.nextInt(10) == 0;           // ~10% late events
                txn.eventTime = late ? now - 20_000 : now;        // 20s in the past

                String json = JsonUtil.toJson(txn);
                producer.send(new ProducerRecord<>("transactions", txn.userId, json));

                if (++sent % 10 == 0) {
                    System.out.println("sent " + sent + " | last: " + json + (late ? "  <-- LATE" : ""));
                }
                Thread.sleep(500); // ~2 events/second, easy to read while learning
            }
        }
    }
}
