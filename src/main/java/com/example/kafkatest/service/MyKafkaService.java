package com.example.kafkatest.service;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

@Service
public class MyKafkaService {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private static final Logger LOGGER = LoggerFactory.getLogger(MyKafkaService.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private String topic = "pupa";

    private KafkaStreams streams;


    public String test() {
        return "test ok";
    }

    public void createMessage(String key) {
        kafkaTemplate.send(topic, key, UUID.randomUUID().toString());
    }

    public void streamPipe() {
        Properties props = getStreamProperties();

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("pupa").to("butterfly");

        final Topology topology = builder.build();

        LOGGER.info("topology: {}", topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        try {
            streams.start();
            latch.await();
        } catch (Throwable ex) {
            ex.printStackTrace();
            streams.close();
            latch.countDown();
        }

    }

    public void streamWithTransformation() {
        Properties props = getStreamProperties();
        final Serde<String> stringSerde = Serdes.String();

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> testStream = builder.stream("pupa", Consumed.with(stringSerde, stringSerde));

        KStream<String, String> firstBlock = testStream
                .mapValues(value -> {
                    String processedValue = value.substring(0,7);
                    LOGGER.info("processed value {}", processedValue);
                    return processedValue;
                });

        firstBlock.to("butterfly", Produced.with(stringSerde, stringSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public void closeStreams() {
        streams.close();
    }

    public void streamWithStore() {
        LOGGER.info("INICIANDO streamWithStore");

        final StreamsBuilder builder = new StreamsBuilder();

//        this.streamCountByKey(builder);
        this.streamNoTransform(builder);
//        this.streamAppend(builder);

        streams = new KafkaStreams(builder.build(), this.getStreamProperties());

        streams.cleanUp();

        streams.start();

        LOGGER.info("FIN streamWithStore");
    }

    public void streamCountByKey(StreamsBuilder builder) {
        final KTable<String, Long> kTable =
            builder
                .stream(this.topic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((k, v) -> k)
                .count();

        kTable.toStream().foreach((k, v) -> LOGGER.info("Cantidad de elementos con key {}: {}", k, v));
    }

    private void streamNoTransform(StreamsBuilder builder) {
            builder
                .stream(this.topic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((k, v) -> k)
                .windowedBy(SessionWindows.with(10000))
                .aggregate(
                    (Initializer<ArrayList<String>>) ArrayList::new,
                    (k, v, a) -> {
                        a.add(v);
                        return a;
                    },
                    (k, a1, a2) -> {
                        return a2;
                    }
                ).toStream().foreach((k, v) -> LOGGER.info("Key: {}, Value: {}", k, v));
    }

    private void streamAppend(StreamsBuilder builder) {
        final KTable<String, String> kTable =
            builder
                .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((k, v) -> k)
                .reduce((v, v1) -> v + " " + v1);

        kTable.toStream().foreach((k, v) -> LOGGER.info("key {}, value {}", k, v));

//        builder.table("butterfly", Materialized.as("butter-query"));
    }

    public void queryStore() {
        ReadOnlyKeyValueStore<String, String> keyValueStore =
                streams.store("butter-query", QueryableStoreTypes.keyValueStore());

        KeyValueIterator<String, String> range = keyValueStore.all();

        while (range.hasNext()) {
            KeyValue<String, String> next = range.next();
            LOGGER.info("CONSUMED KEY VALUE: {}. {}", next.key, next.value);
        }

//        // Get the key-value store CountsKeyValueStore
//        ReadOnlyKeyValueStore<String, Long> keyValueStore =
//                streams.store("CountsKeyValueStore", QueryableStoreTypes.keyValueStore());
//
//        // Get value by key
//        System.out.println("count for hello:" + keyValueStore.get("hello"));
//
//        // Get the values for a range of keys available in this application instance
//        KeyValueIterator<String, Long> range = keyValueStore.range("all", "streams");
//        while (range.hasNext()) {
//            KeyValue<String, Long> next = range.next();
//            System.out.println("count for " + next.key + ": " + value);
//        }
//
//        // Get the values for all of the keys available in this application instance
//        KeyValueIterator<String, Long> range = keyValueStore.all();
//        while (range.hasNext()) {
//            KeyValue<String, Long> next = range.next();
//            System.out.println("count for " + next.key + ": " + value);
//        }
    }

    private Properties getStreamProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("processing.guarantee", "exactly_once");
        return props;
    }


}
