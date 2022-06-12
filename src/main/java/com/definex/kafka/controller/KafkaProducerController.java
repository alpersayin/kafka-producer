package com.definex.kafka.controller;

import com.definex.kafka.service.KafkaProducerService;
import com.opencsv.exceptions.CsvException;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@AllArgsConstructor
@RestController
@RequestMapping("/api")
public class KafkaProducerController {
    private final KafkaProducerService kafkaProducerService;

    @PostMapping("/start")
    public String start(@RequestParam String topicName) throws IOException, CsvException {
        Map<String, Long> result =  kafkaProducerService.start(topicName);
        Map.Entry<String, Long> entry = result.entrySet()
                .stream()
                .findFirst()
                .get();
        return "Producing completed with ProcessID : " + entry.getKey() + " by Time : " + entry.getValue() + " s";
    }

    @PostMapping("/start-test")
    public String startTest(@RequestParam String topicName, Long numberOfId) {
        Map<String, Long> result =  kafkaProducerService.startTest(topicName, numberOfId);
        Map.Entry<String, Long> entry = result.entrySet()
                .stream()
                .findFirst()
                .get();
        return "Producing completed with ProcessID : " + entry.getKey() + " by Time : " + (double) entry.getValue() + " s";
    }

    @PostMapping("/produce")
    public String produce(@RequestParam String topicName, String message) {
        kafkaProducerService.sendMessage(topicName, message);
        return "Producing completed..";
    }

}
