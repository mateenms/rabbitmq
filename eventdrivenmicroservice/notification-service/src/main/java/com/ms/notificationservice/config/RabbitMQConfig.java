package com.ms.notificationservice.config;

import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ configuration for notification-service.
 * Sets Jackson as the message converter so incoming JSON messages
 * are automatically deserialized into OrderEvent.
 *
 * @author Mohammad Mateen
 */
@Configuration
public class RabbitMQConfig {

    /**
     * Registers Jackson as the message converter for JSON deserialization.
     */
    @Bean
    public MessageConverter jacksonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    /**
     * Configures the listener container factory to use Jackson converter.
     * This allows @RabbitListener to receive OrderEvent objects directly.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jacksonMessageConverter());
        return factory;
    }
}
