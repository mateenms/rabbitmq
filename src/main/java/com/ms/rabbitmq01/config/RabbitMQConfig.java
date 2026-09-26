package com.ms.rabbitmq01.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    @Value("${rabbitmq.queue.name}")
    private String queueName;

    @Value("${rabbitmq.exchange.name}")
    private String exchangeName;

    @Value("${rabbitmq.routing.key}")
    private String routingKey;

    @Value("${rabbitmq.json.queue.name}")
    private String jsonQueueName;

    @Value("${rabbitmq.json.exchange.name}")
    private String jsonExchangeName;

    @Value("${rabbitmq.json.routing.key}")
    private String jsonRoutingKey;

    // spring bean rabbit mq queue
    @Bean
    public Queue queue() {
        return new Queue(queueName);
    }

    // spring bean rabbit mq exchange
    @Bean
    public TopicExchange exchange() {
        return new TopicExchange(exchangeName);
    }

    // spring bean rabbit mq binding
    @Bean
    public Binding binding() {
        return BindingBuilder.bind(queue()).to(exchange()).with(routingKey);
    }

    // spring bean rabbit mq json queue
    @Bean
    public Queue jsonQueue() {
        return new Queue(jsonQueueName);
    }

    // spring bean rabbit mq json exchange
    @Bean
    public TopicExchange jsonExchange() {
        return new TopicExchange(jsonExchangeName);
    }

    // spring bean rabbit mq json binding
    @Bean
    public Binding jsonBinding() {
        return BindingBuilder.bind(jsonQueue()).to(jsonExchange()).with(jsonRoutingKey);
    }

    // message converter
    @Bean
    public MessageConverter converter() {
        return new Jackson2JsonMessageConverter();
    }

    // rabbit template
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(converter());
        return rabbitTemplate;
    }

}

