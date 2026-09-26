package com.ms.rabbitmq01.publisher;

import com.ms.rabbitmq01.dto.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class RabbitMQJsonProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQJsonProducer.class);

    @Value("${rabbitmq.json.exchange.name}")
    private String exchange;

    @Value("${rabbitmq.json.routing.key}")
    private String routingKey;

    private RabbitTemplate rabbitTemplate;

    @Autowired
    public RabbitMQJsonProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sendJsonMessage(User user) {
        LOGGER.info(String.format("JSON Message sent -> %s", user.toString()));
        rabbitTemplate.convertAndSend(exchange, routingKey, user);
    }

}
