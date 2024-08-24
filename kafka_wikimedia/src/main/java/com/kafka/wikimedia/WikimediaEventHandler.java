package com.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaEventHandler implements EventHandler {

    private final Logger logger = LoggerFactory.getLogger(WikimediaEventHandler.class);
    private KafkaProducer<String, String> producer;
    private String topic;

    WikimediaEventHandler(KafkaProducer<String, String> producer, String topic) {
        this.topic = topic;
        this.producer = producer;
    }

    static void configureLogger() {
        String log4jConfPath = "src/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
    }


    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        // asynchronous
        logger.info("data: " + messageEvent.getData());
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("Error in stream reading: " + throwable);
    }
}