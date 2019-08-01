package com.example.kafkatest.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class LoggerProcessorWithPunctuation implements Processor<String, ArrayList<String>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerProcessorWithPunctuation.class);

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        KeyValueStore<String, ArrayList<String>> kvStore = (KeyValueStore<String, ArrayList<String>>) this.context.getStateStore("movement-store");

        this.context.schedule(TimeUnit.SECONDS.toMillis(10), PunctuationType.STREAM_TIME, (timestamp) -> {
            LocalDateTime punctuationTime =
                LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(timestamp),
                    ZoneOffset.UTC
                );

            kvStore.all().forEachRemaining((elem -> {
                LOGGER.info("Punctuation. Key: {}, Value: {}, Punctuation time: {}, Current time: {}, Record time: {}", "", "", punctuationTime, LocalDateTime.now(), LocalDateTime.now());
            }));
        });
    }

    @Override
    public void process(String key, ArrayList<String> value) {
        /*LocalDateTime recordTimestamp =
            LocalDateTime.ofInstant(
                Instant.ofEpochMilli(this.context.timestamp()),
                ZoneOffset.UTC
            );

        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);

        this.context.schedule(TimeUnit.SECONDS.toMillis(10), PunctuationType.STREAM_TIME, (timestamp) -> {
            LocalDateTime punctuationTime =
                LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(timestamp),
                    ZoneOffset.UTC
                );
            LOGGER.info("Punctuation. Key: {}, Value: {}, Punctuation time: {}, Current time: {}, Record time: {}", key, value, punctuationTime, now, recordTimestamp);
        });*/
    }

    @Override
    public void close() {
        //No cleanup needed
    }
}
