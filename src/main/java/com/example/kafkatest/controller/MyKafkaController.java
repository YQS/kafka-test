package com.example.kafkatest.controller;

import com.example.kafkatest.service.MyKafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("kafka")
public class MyKafkaController {

    @Autowired
    private MyKafkaService myKafkaService;


    @GetMapping
    public ResponseEntity<String> test() {
        return new ResponseEntity<>(myKafkaService.test(), HttpStatus.OK);
    }

    @RequestMapping(value = "create/{key}", method = RequestMethod.GET)
    public void createMessage(@PathVariable("key") String key) {
        myKafkaService.createMessage(key);
    }

    @RequestMapping(value = "stream", method = RequestMethod.GET)
    public void stream() {
        myKafkaService.streamWithTransformation();
    }

    @RequestMapping(value = "store", method = RequestMethod.GET)
    public void storeStream() {
        myKafkaService.streamWithStore();
    }

    @RequestMapping(value = "close", method = RequestMethod.GET)
    public void closeStream() {
        myKafkaService.closeStreams();
    }

    @RequestMapping(value = "query", method = RequestMethod.GET)
    public void query() {
        myKafkaService.queryStore();
    }

}
