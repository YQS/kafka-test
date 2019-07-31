package com.example.kafkatest.controller;

import com.example.kafkatest.service.CustomKafkaProducer;
import com.example.kafkatest.service.StreamManagement;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class KafkaTestController {

    @Autowired
    private StreamManagement myKafkaService;

    @Autowired
    private CustomKafkaProducer customKafkaProducer;

    @RequestMapping(value = "message/{key}", method = RequestMethod.POST)
    public void sendMessage(@PathVariable("key") String key) {
        customKafkaProducer.createMessage(key);
    }
}
