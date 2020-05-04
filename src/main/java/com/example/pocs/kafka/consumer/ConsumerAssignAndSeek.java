package com.example.pocs.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerAssignAndSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerAssignAndSeek.class);

        //Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /*
         For Assign and Seek there is no need of group id and topic
         Basically used for re-reading data from certain partition and offset
         */

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Assign
        TopicPartition topicPartition = new TopicPartition("first_topic", 0);
        consumer.assign(Arrays.asList(topicPartition));

        //Seek
        long offsetToReadFrom = 5L;
        consumer.seek(topicPartition, offsetToReadFrom);

        int messagesToRead = 5;
        int messageRead = 0;
        boolean moreToRead = true;
        //Poll for data
        while (moreToRead) {

            //Here the consumer will read from each partition completely and then go to the next partition
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records) {
                logger.info("Key: " + record.key() + "\n" +
                    "Value: " + record.value() + "\n" +
                    "Partition: " + record.partition() + "\n" +
                    "Offset: " + record.offset());

                messageRead++;

                if (messageRead==messagesToRead) {
                    moreToRead = false;
                    break;
                }
            }
        }
    }
}
