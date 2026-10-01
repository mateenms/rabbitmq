package com.ms.notificationservice.consumer;

import com.ms.notificationservice.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

/**
 * Listens to notification.send.queue and processes incoming order events.
 * Responsible for sending order confirmation notifications (email/SMS).
 *
 * @author Mohammad Mateen
 */
@Service
public class NotificationConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationConsumer.class);

    /**
     * Consumes OrderEvent from notification.send.queue.
     * Logs the order details and simulates sending a confirmation notification.
     */
    @RabbitListener(queues = "${rabbitmq.queue.notification.name}")
    public void consumeOrderEvent(OrderEvent orderEvent) {
        LOGGER.info("Notification service received event from notification.send.queue");
        LOGGER.info("  Order ID  : {}", orderEvent.getOrder().getOrderId());
        LOGGER.info("  Item      : {}", orderEvent.getOrder().getName());
        LOGGER.info("  Quantity  : {}", orderEvent.getOrder().getQuantity());
        LOGGER.info("  Price     : {}", orderEvent.getOrder().getPrice());
        LOGGER.info("  Status    : {}", orderEvent.getStatus());
        LOGGER.info("  Message   : {}", orderEvent.getMessage());
        LOGGER.info("Notification sent successfully for orderId: {}", orderEvent.getOrder().getOrderId());
    }
}
