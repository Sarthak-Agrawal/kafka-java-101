package com.example.pocs.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWithThread {

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);


        public ConsumerRunnable(CountDownLatch latch, String topic, Properties properties) {
            this.latch = latch;
            consumer = new KafkaConsumer<>(properties);
            //Subscribe to a topic(s)
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {

                    //Here the consumer will read from each partition completely and then go to the next partition
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal.");
            } finally {
                consumer.close();
                // Tell the main code that the consumer is done
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw an exception WakeUpException
            consumer.wakeup();
        }
    }

    public static void main(String[] args) {
        new ConsumerWithThread().run();
    }

    private ConsumerWithThread(){

    }

    private void run() {

        Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);

        String groupId = "my-java-consumer";

        //Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Latch for dealing with multiple threads
        CountDownLatch countDownLatch = new CountDownLatch(1);

        // Creating Consumer Runnable
        logger.info("Creating consumer thread:");
        Runnable consumerRunnable = new ConsumerRunnable(countDownLatch, "first_topic", properties);

        // Start the thread
        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
            () -> {
                logger.info("Caught shutdown Hook");

                ((ConsumerRunnable) consumerRunnable).shutdown();

                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                logger.info("Application has exited.");
            }
        ));


        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted. ", e);
        } finally {
            logger.info("Application closing");
        }
    }
}
