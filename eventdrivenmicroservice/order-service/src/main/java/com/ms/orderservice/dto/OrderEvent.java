package com.ms.orderservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Event published to RabbitMQ when an order is placed.
 * Consumed by inventory-service and notification-service.
 *
 * @author Mohammad Mateen
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {

    private String status;
    private String message;
    private OrderDTO order;

}
