package com.definex.kafka.service;

import com.hazelcast.core.HazelcastInstance;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
@Slf4j
@AllArgsConstructor
public class KafkaProducerService {

    private KafkaTemplate<String, String> kafkaTemplate;
    private final HazelcastInstance hazelcastInstance;

    public Map<String, Long> start(String topicName) throws IOException, CsvException {
        String uuid = getUuid();

        cacheAlandIds(uuid);

        String[] musteriIDs = getMusteriIDs();

        long startTime = System.nanoTime();
        Arrays.stream(musteriIDs)
                .forEach(e -> sendMessage(topicName, uuid, e));
        long endTime   = System.nanoTime();
        long totalTime = endTime - startTime;
        log.info("#### -> Messaging end with Time: {} s", totalTime/1_000_000_000);

        return Collections.singletonMap(uuid, totalTime/1_000_000_000);
    }

    public Map<String, Long> startTest(String topicName, Long numberOfId) {
        String uuid = getUuid();

        cacheAlandIds(uuid);

        List<String> musteriIDs = createMusterIds(numberOfId);

        long startTime = System.nanoTime();
        musteriIDs.forEach(e -> sendMessage(topicName, uuid, e));
        long endTime   = System.nanoTime();
        long totalTime = endTime - startTime;
        log.info("#### -> Messaging end with Time: {} s", totalTime/1_000_000_000);

        return Collections.singletonMap(uuid, totalTime/1_000_000_000);
    }

    private String getUuid() {
        return String.valueOf(UUID.randomUUID());
    }

    public void cacheAlandIds(String processId) {
        Map<String, String> hazelcastMap = hazelcastInstance.getMap("mapList");
        List<String> alanIds = getAlanIDs();
        for (String id: alanIds) {
            hazelcastMap.put(id, processId);
        }

        for (Map.Entry<String, String> entry : hazelcastMap.entrySet()) {
            log.info(entry.getKey() + ":" + entry.getValue());
        }
        System.out.println("#### -> Caching end..");
    }

    private List<String> getAlanIDs() {
        StringBuilder alan = new StringBuilder("Alan");
        List<String> alanIds = new ArrayList<>();
        for (int i = 1; i < 21; i++) {
            alanIds.add(String.valueOf(alan.append(i)));
            alan = new StringBuilder("Alan");
        }
        return  alanIds;
    }

    public String[] getMusteriIDs() throws IOException, CsvException {

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream  = classloader.getResourceAsStream("MOCK_DATA.csv");

        assert inputStream != null;
        InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(streamReader);

        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withSkipLines(1)
                .build();

        List<String[]> allData = csvReader.readAll();

        List<String> allList = new ArrayList<>();
        allData.forEach(e -> allList.add(e[0]));
        String[] myArray = new String[allList.size()];
        allList.toArray(myArray);

        return myArray;
    }

    List<String> createMusterIds(Long numberOfId) {
        List<String> musteriIDs = new ArrayList<>();
        for (long i = 1; i <= numberOfId; i++) {
            musteriIDs.add(String.valueOf(i));
        }
        return musteriIDs;
    }

    public void sendMessage(String topicName, String processId, String message) {
        log.info(String.format("#### -> Producing message -> %s - %s", processId, message));
        kafkaTemplate.send(topicName, processId, message);
    }

    public void sendMessage(String topicName, String message) {
        String processId = getUuid();
        log.info(String.format("#### -> Producing message -> %s - %s", processId, message));
        kafkaTemplate.send(topicName, processId, message);
    }

}
