package com.ms.rabbitmq01.controller;

import com.ms.rabbitmq01.dto.User;
import com.ms.rabbitmq01.publisher.RabbitMQJsonProducer;
import com.ms.rabbitmq01.publisher.RabbitMQProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller exposing endpoints to publish string and JSON messages to RabbitMQ.
 *
 * @author mateen
 */
@RestController
@RequestMapping("/api/v1")
public class MessageController {

    private RabbitMQProducer producer;
    private RabbitMQJsonProducer jsonProducer;

    @Autowired
    public MessageController(RabbitMQProducer producer, RabbitMQJsonProducer jsonProducer) {
        this.producer = producer;
        this.jsonProducer = jsonProducer;
    }

    // publishes a plain string message — GET /api/v1/publish?message=hello
    @GetMapping("/publish")
    public ResponseEntity<String> sendMessage(@RequestParam("message") String message) {
        producer.sendMessage(message);
        return ResponseEntity.ok("Message sent to RabbitMQ..");
    }

    // publishes a User object as JSON — POST /api/v1/publish-json
    @PostMapping("/publish-json")
    public ResponseEntity<String> sendJsonMessage(@RequestBody User user) {
        jsonProducer.sendJsonMessage(user);
        return ResponseEntity.ok("JSON Message sent to RabbitMQ..");
    }

}
