package com.ms.orderservice.publisher;

import com.ms.orderservice.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Publishes order events to RabbitMQ exchange.
 *
 * @author Mohammad Mateen
 */
@Service
public class OrderProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderProducer.class);

    @Value("${rabbitmq.exchange.name}")
    private String orderExchangeName;

    @Value("${rabbitmq.routing.key.order}")
    private String orderRoutingKey;

    @Value("${rabbitmq.routing.key.inventory}")
    private String inventoryRoutingKey;

    @Value("${rabbitmq.routing.key.notification}")
    private String notificationRoutingKey;

    private final AmqpTemplate amqpTemplate;

    @Autowired
    public OrderProducer(AmqpTemplate amqpTemplate) {
        this.amqpTemplate = amqpTemplate;
    }

    /**
     * Publishes an OrderEvent to order, inventory and notification queues via the exchange.
     * Throws AmqpException if RabbitMQ is unavailable.
     */
    public void sendOrderEvent(OrderEvent orderEvent) {
        LOGGER.info("Publishing order event -> orderId: {}, status: {}",
                orderEvent.getOrder().getOrderId(),
                orderEvent.getStatus());
        try {
            amqpTemplate.convertAndSend(orderExchangeName, orderRoutingKey, orderEvent);
            amqpTemplate.convertAndSend(orderExchangeName, inventoryRoutingKey, orderEvent);
            amqpTemplate.convertAndSend(orderExchangeName, notificationRoutingKey, orderEvent);
            LOGGER.info("Order event published to order, inventory and notification queues for orderId: {}",
                    orderEvent.getOrder().getOrderId());
        } catch (AmqpException e) {
            LOGGER.error("Failed to publish order event for orderId: {} — {}",
                    orderEvent.getOrder().getOrderId(), e.getMessage());
            throw e;
        }
    }

}
