package com.ms.inventoryservice.consumer;

import com.ms.inventoryservice.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

/**
 * Listens to order.placed.queue and processes incoming order events.
 * Responsible for reducing stock based on the received order.
 *
 * @author Mohammad Mateen
 */
@Service
public class InventoryConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(InventoryConsumer.class);

    /**
     * Consumes OrderEvent from order.placed.queue.
     * Logs the order details and simulates a stock reduction.
     */
    @RabbitListener(queues = "order.placed.queue")
    public void consumeOrderEvent(OrderEvent orderEvent) {
        LOGGER.info("Inventory service received event from order.placed.queue");
        LOGGER.info("  Order ID  : {}", orderEvent.getOrder().getOrderId());
        LOGGER.info("  Item      : {}", orderEvent.getOrder().getName());
        LOGGER.info("  Quantity  : {}", orderEvent.getOrder().getQuantity());
        LOGGER.info("  Price     : {}", orderEvent.getOrder().getPrice());
        LOGGER.info("  Status    : {}", orderEvent.getStatus());
        LOGGER.info("  Message   : {}", orderEvent.getMessage());
        LOGGER.info("Stock reduced successfully for orderId: {}", orderEvent.getOrder().getOrderId());
    }
}
