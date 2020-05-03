package com.example.pocs.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeys {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10; i++) {
            //create a producer record
            String key = "id_" + i;
            ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("first_topic", key, "Hello World " + i);

            //send Data - asynchronous
            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Successfully produced data. \n" +
                        "Topic: " + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset());
                } else {
                    logger.error("Error while producing data: ", e);
                }
            });
        }

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
