package com.example.kafkatest.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;

public class LoggerProcessor implements Processor<String, ArrayList<String>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerProcessor.class);

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

        LocalDateTime now = LocalDateTime.now();

        if(now.minusSeconds(10).compareTo(recordTimestamp) > 0) {
            LOGGER.info("Time difference greater than 10 seconds. Key: {}, Value: {}, Current time: {}, Record time: {}", key, value, now, recordTimestamp);
        } else {
            LOGGER.info("Time difference lesser than or equal to 10 seconds. Key: {}, Value: {}, Current time: {}, Record time: {}", key, value, now, recordTimestamp);
        }
    }

    @Override
    public void close() {
        //No cleanup needed
    }
}
