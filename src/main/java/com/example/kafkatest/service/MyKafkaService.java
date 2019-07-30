package com.example.kafkatest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Service
public class MyKafkaService {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private static final Logger LOGGER = LoggerFactory.getLogger(MyKafkaService.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private String topic = "pupa";
    private String storeName = "pupa-query";

    private KafkaStreams streams;

    private ObjectMapper objectMapper = new ObjectMapper();


    public String test() {
        return "test ok";
    }

    public void createMessage(String key) {
        kafkaTemplate.send(topic, key, UUID.randomUUID().toString());
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
//        this.streamNoTransform(builder);
        this.streamAppend(builder);

        streams = new KafkaStreams(builder.build(), this.getStreamProperties());

        streams.cleanUp();

        streams.start();

        LOGGER.info("FIN streamWithStore");
    }

    private void streamAppend(StreamsBuilder builder) {
        /*builder
            .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.with(TimeUnit.SECONDS.toMillis(10)))
            .aggregate(
                () -> new String("[]"),
                (k, v, a) -> {
                    List<String> list = null;
                    String result = null;

                    try {
                        list = (List<String>) objectMapper.readValue(a, List.class);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    list.add(v);
                    try {
                        result = objectMapper.writeValueAsString(list);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }

                    return result;
                },
                (k, a1, a2) -> {
                    List<String> list1 = null;
                    List<String> list2 = null;
                    List<String> array = new ArrayList<>();
                    String result = null;

                    try {
                        list1 = (List<String>) objectMapper.readValue(a1, List.class);
                        list2 = (List<String>) objectMapper.readValue(a2, List.class);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    array.addAll(list1);
                    array.addAll(list2);
                    try {
                        result = objectMapper.writeValueAsString(array);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }

                    return result;
                }
            )
            .toStream().foreach((k, v) -> LOGGER.info("Key: {}, Value: {}", k.key(), v));*/

        builder
            .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .aggregate(
                    () -> new String("[]"),
                    (k, v, a) -> {
                        List<String> list = null;
                        String result = null;

                        try {
                            list = (List<String>) objectMapper.readValue(a, List.class);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        list.add(v);
                        try {
                            result = objectMapper.writeValueAsString(list);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }

                        return result;
                    },
                    Materialized.as(storeName)
            )
            .toStream()
            .process(() ->
                new Processor<>() {
                    private ProcessorContext context;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        //this.timestamp =
                         //   LocalDateTime
                          //      .ofInstant(
                           //         Instant.ofEpochMilli(context.timestamp()),
                            //        ZoneOffset.UTC
                             //   );
                        //context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, new Punctuator(..)); // punctuate each 1000ms, can access this.state
                    }

                    @Override
                    public void process(String key, String value) {
                        LocalDateTime timestamp =
                                LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(this.context.timestamp()),
                                        ZoneOffset.UTC
                                );

                        LOGGER.info("comparing to timestamp with value {}", this.context.timestamp());
                        if(LocalDateTime.now().minusSeconds(10).compareTo(timestamp) > 0) {
                            LOGGER.info("KEY: {}, VALUE: {}", key, value);
                        }
                    }

                    @Override
                    public void close() {
                    }
                }
            );
    }

    public Map<String, Object> queryStore() {
        ReadOnlyKeyValueStore<String, String> keyValueStore =
            streams.store(storeName, QueryableStoreTypes.keyValueStore());

        KeyValueIterator<String, String> range = keyValueStore.all(); //keyValueStore.range(keyInicial, keyFinal)

        Map<String, Object> result = new HashMap<>();

        while (range.hasNext()) {
            KeyValue<String, String> next = range.next();
            LOGGER.info("CONSUMED KEY VALUE: {}. {}", next.key, next.value);

            result.put(next.key, next.value);
        }

        return result;
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
