package com.tryton.tut.tut_spring_kafka_consumer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.apachecommons.CommonsLog;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@CommonsLog
public class Producer {

    private static final String TOPIC = "users";

    private final KafkaTemplate kafkaTemplate;

    public void sendMessage(String message) {
        log.info(String.format("Producing message: %s", message));
        kafkaTemplate.send(TOPIC, message);
    }
}
