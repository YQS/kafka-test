package com.example.kafkatest.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class EventList {
    private List<Event> events = new ArrayList<>();

    public EventList() {
    }

    public EventList add(Event event) {
        this.events.add(event);
        return this;
    }

    public List<Event> getAll() {
        return events;
    }

    public Event get(Integer index) {
        return events.get(index);
    }

    public Event first() {
        return this.events.get(0);
    }

    public Event last() {
        return this.events.get(this.events.size() - 1);
    }

    public LocalDateTime getTimestamp() {
        return this.last().getTimestamp();
    }

    public Integer size() {
        return this.events.size();
    }

    @Override
    public String toString() {
        return "EventList{" +
            "events=" + events +
            '}';
    }
}
