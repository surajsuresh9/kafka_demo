package com.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class);

    static void configureLogger() {
        String log4jConfPath = "src/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
    }

    public static void main(String[] args) {
        configureLogger();
        log.info("hello world");
        String groupId = "my_java_application";
        String topic = "demo_topic";

        // create producer properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // set consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());


        // create consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Shutdown hook
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Initiated shutdown, exiting by calling consumer.wakeup()");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe to topic(s)
            consumer.subscribe(Arrays.asList(topic));

            // get data
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    log.info("Key: " + record.key() + "\tValue: " + record.value());
                    log.info("Partition: " + record.partition() + "\tOffset: " + record.offset());
                }
            }
        } catch (WakeupException ex) {
            log.info("Consumer is shutting down");
        } catch (Exception e) {
            log.info("Unexpected exception");
        } finally {
            consumer.close(); // close the consumer, this will commit the offsets
            log.info("Consumer is shut down gracefully");
        }
    }
}
