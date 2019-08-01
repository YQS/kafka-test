package com.example.kafkatest.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class Event {
    private String id;
    private String value;

    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime timestamp = LocalDateTime.now(ZoneOffset.UTC);

    public Event() {

    }

    public String getId() {
        return id;
    }

    public Event withId(String id) {
        this.id = id;
        return this;
    }

    public String getValue() {
        return value;
    }

    public Event withValue(String value) {
        this.value = value;
        return this;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
            "id='" + id + '\'' +
            ", value='" + value + '\'' +
            ", timestamp=" + timestamp +
            '}';
    }
}
