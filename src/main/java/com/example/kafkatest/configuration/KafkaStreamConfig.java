package com.example.kafkatest.configuration;

import com.example.kafkatest.processor.LoggerProcessor;
import com.example.kafkatest.service.CustomKafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@EnableKafka
@EnableKafkaStreams
@Configuration
public class KafkaStreamConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomKafkaConsumer.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.movement.name}")
    private String topic;

    @Value("${kafka.topic.movement-processing.name}")
    private String processingTopic;

    @Value("${kafka.topic.movement.stream.store}")
    private String storeName;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration streamConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, String> kafkaStreamGroupByKey(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));

        stream
            .groupByKey()
            .aggregate(
                ArrayList::new,
                (k, v, a) -> {
                    a.add(v);
                    return a;
                },
                Materialized
                    .<String, ArrayList<String>>as(Stores.inMemoryKeyValueStore(storeName))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(new JsonSerde<>(ArrayList.class))
            )
            .toStream()
            .to(processingTopic, Produced.with(Serdes.String(), new JsonSerde<>(ArrayList.class)));

        return stream;
    }

    @Bean
    public KStream<String, ArrayList<String>> kafkaStreamProcess(StreamsBuilder streamsBuilder) {
        KStream<String, ArrayList<String>> stream = streamsBuilder.stream(processingTopic, Consumed.with(Serdes.String(), new JsonSerde<>(ArrayList.class)));

        stream
            .process(LoggerProcessor::new);

        return stream;
    }
}
