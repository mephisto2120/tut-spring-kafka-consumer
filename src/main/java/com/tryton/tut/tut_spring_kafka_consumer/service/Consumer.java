package com.tryton.tut.tut_spring_kafka_consumer.service;

import lombok.extern.apachecommons.CommonsLog;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@CommonsLog
public class Consumer {
    @KafkaListener(topics = "wikimedia.recentchange", groupId = "consumer-opensearch-demo-X")
    public void consume(String message) {
        log.info(String.format("Consumed message: %s", message));
    }
}
