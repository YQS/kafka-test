package com.example.kafkatest.configuration;

import com.example.kafkatest.model.Event;
import com.example.kafkatest.model.EventList;
import com.example.kafkatest.processor.LoggerProcessorOtherTopic;
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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
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

    @Value("${kafka.topic.movement.stream.store.aggregate}")
    private String aggregationStoreName;

    @Value("${kafka.topic.movement.stream.store.reduce}")
    private String reduceStoreName;

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
    @SuppressWarnings("all")
    public KStream<String, Event> kafkaStreamSendToAnotherTopic(StreamsBuilder streamsBuilder) {
        KStream<String, Event> stream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), new JsonSerde<>(Event.class)));

        KStream<String, EventList>[] streamArray =
            stream
                .groupByKey()
                .aggregate(
                    EventList::new,
                    (k, v, a) -> a.add(v),
                    Materialized.with(Serdes.String(), new JsonSerde<>(EventList.class))
                )
                .toStream()
                .groupByKey()
                .reduce(
                    (v1, v2) -> {
                        if(v1.size() < v2.size()) {
                            return v2;
                        } else {
                            return v1;
                        }
                    },
                    Materialized.with(Serdes.String(), new JsonSerde<>(EventList.class))
                )
                .toStream()
                .branch((k, v) -> LocalDateTime.now(ZoneOffset.UTC).minusSeconds(10).compareTo(v.getTimestamp()) > 0, (k, v) -> LocalDateTime.now(ZoneOffset.UTC).minusSeconds(10).compareTo(v.getTimestamp()) <= 0);

        streamArray[0].foreach((k, v) -> LOGGER.info("Record en consumer stream con suppression. Key: {}, Value: {}", k, v));
        streamArray[1].to(processingTopic, Produced.with(Serdes.String(), new JsonSerde<>(EventList.class)));

        return stream;
    }
}
