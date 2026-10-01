package com.ms.orderservice.config;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ configuration for order-service.
 * Declares exchange, queues, bindings, message converter and RabbitTemplate.
 *
 * @author Mohammad Mateen
 */
@Configuration
public class RabbitMQConfig {

    @Value("${rabbitmq.exchange.name}")
    private String orderEventsExchangeName;

    @Value("${rabbitmq.queue.order.name}")
    private String orderPlacedQueueName;

    @Value("${rabbitmq.queue.inventory.name}")
    private String inventoryUpdateQueueName;

    @Value("${rabbitmq.queue.notification.name}")
    private String notificationSendQueueName;

    @Value("${rabbitmq.routing.key.order}")
    private String orderPlacedRoutingKey;

    @Value("${rabbitmq.routing.key.inventory}")
    private String inventoryUpdateRoutingKey;

    @Value("${rabbitmq.routing.key.notification}")
    private String notificationSendRoutingKey;

    /**
     * Declares a TopicExchange where order events are published.
     * All queues (order, inventory, notification) are bound to this exchange.
     */
    @Bean
    public TopicExchange orderExchange() {
        return new TopicExchange(orderEventsExchangeName);
    }

    /**
     * Declares a durable queue for storing placed order events.
     * Consumed by order-service itself for tracking purposes.
     */
    @Bean
    public Queue orderQueue() {
        return new Queue(orderPlacedQueueName);
    }

    /**
     * Declares a durable queue for inventory update events.
     * Consumed by inventory-service to reduce stock on order placement.
     */
    @Bean
    public Queue inventoryQueue() {
        return new Queue(inventoryUpdateQueueName);
    }

    /**
     * Declares a durable queue for notification events.
     * Consumed by notification-service to send order confirmation email/SMS.
     */
    @Bean
    public Queue notificationQueue() {
        return new Queue(notificationSendQueueName);
    }

    /**
     * Binds the order queue to the exchange using the order routing key.
     * Messages published with this routing key are routed to order queue.
     */
    @Bean
    public Binding orderQueueBinding() {
        return BindingBuilder.bind(orderQueue()).to(orderExchange()).with(orderPlacedRoutingKey);
    }

    /**
     * Binds the inventory queue to the exchange using the inventory routing key.
     * Messages published with this routing key are routed to inventory queue.
     */
    @Bean
    public Binding inventoryQueueBinding() {
        return BindingBuilder.bind(inventoryQueue()).to(orderExchange()).with(inventoryUpdateRoutingKey);
    }

    /**
     * Binds the notification queue to the exchange using the notification routing key.
     * Messages published with this routing key are routed to notification queue.
     */
    @Bean
    public Binding notificationQueueBinding() {
        return BindingBuilder.bind(notificationQueue()).to(orderExchange()).with(notificationSendRoutingKey);
    }

    /**
     * Registers Jackson as the message converter.
     * Automatically serializes and deserializes Java objects to/from JSON.
     */
    @Bean
    public MessageConverter jacksonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    /**
     * Configures AmqpTemplate (backed by RabbitTemplate) with the Jackson message converter.
     * Used by producers to send messages to RabbitMQ exchanges.
     */
    @Bean
    public AmqpTemplate amqpTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(jacksonMessageConverter());
        return rabbitTemplate;
    }

    /**
     * RabbitAdmin is responsible for declaring queues, exchanges and bindings on the broker.
     */
    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    /**
     * Forces RabbitAdmin to initialize on startup — declares all queues and exchanges immediately.
     */
    @Bean
    public ApplicationRunner rabbitInitializer(RabbitAdmin rabbitAdmin) {
        return args -> rabbitAdmin.initialize();
    }

}
