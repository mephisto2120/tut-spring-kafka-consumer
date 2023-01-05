package com.tryton.tut.tut_spring_kafka_consumer.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.apachecommons.CommonsLog;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/home")
@CommonsLog
@Tag(name = "HomeController", description = "Rest Template Service HomeController")
public class HomeController {

    @GetMapping
    @Operation(summary = "First endpoint in rest template service")
    public DummyObject getHome() {
        log.info("Starting getHome.");
        //call service
        DummyObject dummyObject = new DummyObject(1, new DummyObject.DummyNestedObject("OHH! It works!"));
        log.info("Finished getHome.");
        return dummyObject;
    }

    @GetMapping("error")
    @Operation(summary = "Throws RuntimeException - to test Errors handling")
    public void testErrorHandling() {
        log.info("Starting testErrorHandling.");
        throw new RuntimeException("Testing Error handling");
    }

    @AllArgsConstructor
    @Getter
    private static class DummyObject {
        private final int id;
        private final DummyNestedObject nested;

        @AllArgsConstructor
        @Getter
        private static class DummyNestedObject {
            private final String name;
        }
    }
}
