package com.example.kafkatest.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CustomKafkaConsumer implements ConsumerSeekAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomKafkaConsumer.class);

    @Value("${kafka.topic.movement.name}")
    private String topic;

    @Value("${kafka.topic.movement-processing.name}")
    private String processingTopic;

    private final Map<TopicPartition, ConsumerSeekCallback> callbacks = new ConcurrentHashMap<>();

    private static final ThreadLocal<ConsumerSeekCallback> callbackForThread = new ThreadLocal<>();

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        callbackForThread.set(callback);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.keySet().forEach(tp -> this.callbacks.put(tp, callbackForThread.get()));
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    }

    @KafkaListener(id = "kafka-test-normal-consumer", topics = "${kafka.topic.movement.name}", concurrency = "3")
    public void listen(List<ConsumerRecord<Integer, String>> value) {
        LOGGER.info("Record en consumer normal: {}", value);
    }

    public void seekToStart() {
        this.callbacks.forEach((topicPartition, callback) -> callback.seekToBeginning(topicPartition.topic(), topicPartition.partition()));
    }

    public void seekToEnd() {
        this.callbacks.forEach((topicPartition, callback) -> callback.seekToEnd(topicPartition.topic(), topicPartition.partition()));
    }

    public void seekToOffset(Long offset) {
        this.callbacks.forEach((topicPartition, callback) -> callback.seek(topicPartition.topic(), topicPartition.partition(), offset));
    }
}
