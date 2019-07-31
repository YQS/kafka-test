package com.example.kafkatest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
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
    private KafkaTemplate<String, String> kafkaTemplate;

    public void createMessage(String key) {
        kafkaTemplate.send(topic, key, UUID.randomUUID().toString());
    }
}