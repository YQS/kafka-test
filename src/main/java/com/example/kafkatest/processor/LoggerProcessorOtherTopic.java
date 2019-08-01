package com.example.kafkatest.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;

public class LoggerProcessorOtherTopic implements Processor<String, ArrayList<String>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerProcessorOtherTopic.class);

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, ArrayList<String> value) {
        LocalDateTime recordTimestamp =
            LocalDateTime.ofInstant(
                Instant.ofEpochMilli(this.context.timestamp()),
                ZoneOffset.UTC
            );

        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);

        if(now.minusSeconds(10).compareTo(recordTimestamp) > 0) {
            LOGGER.info("Processing from the other topic. Key: {}, Value: {}, Current time: {}, Record time: {}", key, value, now, recordTimestamp);
        } else {
            this.context.forward(key, value);
        }
    }

    @Override
    public void close() {
        //No cleanup needed
    }
}