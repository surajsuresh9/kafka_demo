package com.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    static void configureLogger() {
        String log4jConfPath = "src/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
    }

    public static void main(String[] args) {
        configureLogger();
        log.info("hello world");

        // create producer properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        // create producer
        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                // create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_topic", "hello world " + i);

                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                }
                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executed everytime a producer sends a record successfully
                        if (e == null) {
                            log.info("Sent Record\n"
                                    + "Topic: " + recordMetadata.topic() + "\n"
                                    + "Partition: " + recordMetadata.partition() + "\n"
                                    + "Offset: " + recordMetadata.offset() + "\n"
                                    + "Timestamp: " + recordMetadata.timestamp() + "\n");
                        } else {
                            log.info("Fatal! Error occurred while sending record: " + e);
                        }
                    }
                });
            }
        }
        // tell the producer to send all data and block until done --synchronous
        producer.flush();

        // close the producer
        producer.close();
    }
}
