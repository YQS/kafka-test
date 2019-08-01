package com.example.kafkatest.service;

import com.example.kafkatest.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class CustomKafkaProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomKafkaProducer.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.movement.name}")
    private String topic;

    @Autowired
    private KafkaTemplate<String, Event> kafkaTemplate;

    public void createMessage(String key) {
//        Message<Event> message =
//            MessageBuilder
//                .withPayload(new Event().withId(UUID.randomUUID().toString()).withValue(UUID.randomUUID().toString()))
//                .setHeader(KafkaHeaders.TOPIC, topic)
//                .build();
        kafkaTemplate.send(topic, key, new Event().withId(UUID.randomUUID().toString()).withValue(UUID.randomUUID().toString()));
    }
}
