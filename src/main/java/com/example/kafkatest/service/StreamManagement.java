package com.example.kafkatest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class StreamManagement {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamManagement.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.movement.stream.store}")
    private String storeName;

    /*public Map<String, Object> queryStore() {
        ReadOnlyKeyValueStore<String, String> keyValueStore =
            streams.store(storeName, QueryableStoreTypes.keyValueStore());

        KeyValueIterator<String, String> range = keyValueStore.all();

        Map<String, Object> result = new HashMap<>();

        while (range.hasNext()) {
            KeyValue<String, String> next = range.next();
            LOGGER.info("CONSUMED KEY VALUE: {}. {}", next.key, next.value);

            result.put(next.key, next.value);
        }

        return result;
    }*/
}
