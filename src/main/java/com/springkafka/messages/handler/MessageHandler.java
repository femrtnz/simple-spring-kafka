package com.springkafka.messages.handler;

import java.util.concurrent.CountDownLatch;

import com.springkafka.messages.processor.IntegerProcessorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class MessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);

    private CountDownLatch latch = new CountDownLatch(1);

    private static String SENDER_TOPIC = "sender.t";

    @Autowired
    private IntegerProcessorService processor;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = "${kafka.topic.receiver}")
    public void receive(String payload) {
        LOGGER.info("Received message='{}'", payload);
        final String messageReturn = processMessage(payload);
        LOGGER.info("sending payload='{}' to topic='{}'", payload, SENDER_TOPIC);
        kafkaTemplate.send(SENDER_TOPIC, messageReturn);
    }

    private String processMessage(String payload) {
        LOGGER.info("Processing message='{}'", payload);
        final String messageReturn = processor.execute(payload);
        latch.countDown();
        return messageReturn;
    }
}

