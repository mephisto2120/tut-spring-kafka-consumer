package com.tryton.tut.tut_spring_kafka_consumer.controller;

import com.tryton.tut.tut_spring_kafka_consumer.service.WikimediaConsumerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RequiredArgsConstructor
@RestController
@RequestMapping(value = "/wikimedia/consumer/")
public class WikimediaConsumerController {

	private final WikimediaConsumerService wikimediaConsumerService;

	@PostMapping(value = "/start")
	public void start() throws IOException {
		wikimediaConsumerService.start();
	}
}
